import threading
import unittest
from decimal import Decimal

from coldledger.transport.capacity_commit import Book, Capacity
from coldledger.transport.container_ledger import ContainerLedger
from coldledger.storage.event_store import EventStore
from coldledger.transport.leg_booking import (
    BOOKING_KIND,
    BookingConflict,
    LegRequest,
    MultiLegBooking,
)


def Q(value):
    return Decimal(str(value))


def make_book():
    return Book([
        Capacity("leg-a", Q(10), version=0),
        Capacity("leg-b", Q(8), version=0),
        Capacity("leg-c", Q(5), version=0),
    ])


def legs(*specs):
    return [LegRequest(leg_id, account, q) for leg_id, account, q in specs]


class PrimitiveAtomicityTests(unittest.TestCase):
    def test_capacity_failure_on_last_leg_leaves_nothing(self):
        book = make_book()
        with self.assertRaises(ValueError):
            book.reserve_atomic(
                {"leg-a": Q(4), "leg-b": Q(4), "leg-c": Q(9)},
                {"leg-a": 0, "leg-b": 0, "leg-c": 0},
            )
        snap = book.snapshot(["leg-a", "leg-b", "leg-c"])
        self.assertEqual(snap["leg-a"].available, Q(10))
        self.assertEqual(snap["leg-b"].available, Q(8))
        self.assertEqual(snap["leg-c"].available, Q(5))
        self.assertEqual({r.version for r in snap.values()}, {0})

    def test_capacity_version_conflict_leaves_nothing(self):
        book = make_book()
        with self.assertRaises(RuntimeError):
            book.reserve_atomic(
                {"leg-a": Q(1), "leg-b": Q(1), "leg-c": Q(1)},
                {"leg-a": 0, "leg-b": 7, "leg-c": 0},  # stale mid-batch
            )
        snap = book.snapshot(["leg-a", "leg-b", "leg-c"])
        self.assertEqual({r.version for r in snap.values()}, {0})

    def test_capacity_success_is_whole_batch(self):
        book = make_book()
        out = book.reserve_atomic(
            {"leg-a": Q(4), "leg-b": Q(3), "leg-c": Q(2)},
            {"leg-a": 0, "leg-b": 0, "leg-c": 0},
        )
        self.assertEqual(out["leg-a"].available, Q(6))
        self.assertEqual(out["leg-b"].available, Q(5))
        self.assertEqual(out["leg-c"].available, Q(3))
        self.assertEqual({r.version for r in out.values()}, {1})


class MultiLegBookingTests(unittest.TestCase):
    def setUp(self):
        self.book = make_book()
        self.ledger = ContainerLedger()
        self.events = EventStore()
        self.service = MultiLegBooking(self.book, self.ledger, self.events)

    def test_commit_applies_all_three_stores_together(self):
        result = self.service.book(
            "B-1",
            legs(("leg-a", "acc-1", 4), ("leg-b", "acc-1", 3), ("leg-c", "acc-2", 2)),
            "outbound run",
        )
        self.assertEqual(result.leg_ids, ("leg-a", "leg-b", "leg-c"))
        snap = self.book.snapshot(["leg-a", "leg-b", "leg-c"])
        self.assertEqual(snap["leg-a"].available, Q(6))
        self.assertEqual(snap["leg-b"].available, Q(5))
        self.assertEqual(snap["leg-c"].available, Q(3))
        self.assertEqual(self.ledger.balance("acc-1"), Q(7))
        self.assertEqual(self.ledger.balance("acc-2"), Q(2))
        stream = self.events.read("booking:B-1")
        self.assertEqual(len(stream), 1)
        self.assertEqual(stream[0].kind, BOOKING_KIND)
        self.assertEqual(stream[0].payload["reference"], "B-1")

    def test_last_leg_short_leaves_no_half_occupancy_anywhere(self):
        with self.assertRaises(ValueError):
            self.service.book(
                "B-2",
                legs(("leg-a", "acc-1", 4), ("leg-b", "acc-1", 4), ("leg-c", "acc-1", 9)),
                "outbound run",
            )
        # No capacity locked ...
        snap = self.book.snapshot(["leg-a", "leg-b", "leg-c"])
        self.assertEqual([snap[k].available for k in ("leg-a", "leg-b", "leg-c")],
                         [Q(10), Q(8), Q(5)])
        self.assertEqual({r.version for r in snap.values()}, {0})
        # ... no container entries posted ...
        self.assertEqual(self.ledger.entries(), ())
        # ... and no event recorded.
        self.assertEqual(self.events.all_streams(), [])

    def test_version_drift_between_prepare_and_commit_is_retried(self):
        # A competing transaction commits between prepare and the first
        # validate phase. The stale attempt must change nothing; the
        # transparent retry then commits against the new versions.
        class DriftyService(MultiLegBooking):
            def __init__(self, *args):
                super().__init__(*args)
                self.attempts = 0

            def _commit(self, reference, reqs, reason, stream, versions, expected):
                self.attempts += 1
                if self.attempts == 1:
                    # Competitor steals one unit on every leg using fresh versions.
                    self.capacity.reserve_atomic(
                        {leg: Q(1) for leg in reqs},
                        {leg: self.capacity.rows[leg].version for leg in reqs},
                    )
                return super()._commit(reference, reqs, reason, stream, versions, expected)

        service = DriftyService(self.book, self.ledger, self.events)
        result = service.book(
            "B-3", legs(("leg-a", "acc-1", 2), ("leg-b", "acc-1", 2)), "drift"
        )
        self.assertGreaterEqual(service.attempts, 2)
        self.assertEqual(result.legs[0].version, 2)  # competitor + retry
        self.assertEqual(self.book.rows["leg-a"].available, Q(7))   # 10 -1 -2
        self.assertEqual(self.book.rows["leg-b"].available, Q(5))   # 8 -1 -2
        self.assertEqual(len(self.events.read("booking:B-3")), 1)
        # The abandoned attempt produced no ledger rows of its own.
        self.assertEqual(len(self.ledger.entries()), 2)

    def test_event_stream_drift_is_retried(self):
        class EventDriftyService(MultiLegBooking):
            def __init__(self, *args):
                super().__init__(*args)
                self.attempts = 0

            def _commit(self, reference, reqs, reason, stream, versions, expected):
                self.attempts += 1
                if self.attempts == 1:
                    self.events.append(stream, self.events.stream_version(stream), "noise", {})
                return super()._commit(reference, reqs, reason, stream, versions, expected)

        service = EventDriftyService(self.book, self.ledger, self.events)
        # The injected noise still occupies the stream slot; the booking
        # retries against the new length and appends after it.
        result = service.book("B-4", legs(("leg-a", "acc-1", 1)), "drift")
        self.assertEqual(result.event.sequence, 2)
        rows = self.events.read("booking:B-4")
        self.assertEqual([e.kind for e in rows], ["noise", BOOKING_KIND])

    def test_duplicate_reference_is_idempotent(self):
        first = self.service.book("B-5", legs(("leg-a", "acc-1", 2)), "run")
        second = self.service.book("B-5", legs(("leg-a", "acc-1", 2)), "run")
        self.assertIs(first, second)
        self.assertEqual(self.book.rows["leg-a"].available, Q(8))
        self.assertEqual(self.book.rows["leg-a"].version, 1)
        self.assertEqual(len(self.ledger.entries()), 1)
        self.assertEqual(len(self.events.read("booking:B-5")), 1)

    def test_duplicate_reference_from_another_instance_is_permanent(self):
        other = MultiLegBooking(self.book, self.ledger, self.events)
        other.book("B-5b", legs(("leg-a", "acc-1", 2)), "run")
        before_version = self.book.rows["leg-a"].version
        with self.assertRaises(ValueError):
            self.service.book("B-5b", legs(("leg-a", "acc-1", 2)), "run")
        # No retry storm changed anything after the first commit.
        self.assertEqual(self.book.rows["leg-a"].version, before_version)
        self.assertEqual(len(self.ledger.entries()), 1)
        self.assertEqual(len(self.events.read("booking:B-5b")), 1)

    def test_concurrent_bookings_never_oversell_or_leak(self):
        # 30 bookings of 5 units on a 100-unit leg: 20 commit, 10 fail.
        shared_book = Book([Capacity("leg-a", Q(100), version=0)])
        service_factory = lambda: MultiLegBooking(shared_book, self.ledger, self.events)
        errors = []
        results = []
        barrier = threading.Barrier(30)

        def worker(i):
            svc = service_factory()
            barrier.wait()
            try:
                results.append(svc.book(f"C-{i}", [LegRequest("leg-a", f"acc-{i % 3}", 5)], "contended"))
            except ValueError as exc:
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(30)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(results), 20)
        self.assertEqual(len(errors), 10)
        self.assertEqual(shared_book.rows["leg-a"].available, Q(0))
        self.assertEqual(shared_book.rows["leg-a"].version, 20)
        # Each committed booking posted exactly one ledger entry and one event.
        self.assertEqual(len(self.ledger.entries()), 20)
        total = sum((e.quantity for e in self.ledger.entries()), Decimal("0"))
        self.assertEqual(total, Q(100))
        committed_streams = [s for s in self.events.all_streams() if s.startswith("booking:C-")]
        self.assertEqual(len(committed_streams), 20)

    def test_invalid_requests_reject_before_any_store(self):
        with self.assertRaises(ValueError):
            self.service.book("", legs(("leg-a", "acc-1", 1)), "run")
        with self.assertRaises(ValueError):
            self.service.book("B", [], "run")
        with self.assertRaises(ValueError):
            self.service.book("B", legs(("leg-a", "acc-1", 0)), "run")
        with self.assertRaises(ValueError):
            self.service.book("B", [LegRequest("leg-a", "", 1)], "run")
        with self.assertRaises(ValueError):
            self.service.book(
                "B",
                legs(("leg-a", "acc-1", 1), ("leg-a", "acc-1", 1)),
                "run",
            )
        self.assertEqual(self.events.all_streams(), [])
        self.assertEqual(self.ledger.entries(), ())
        self.assertEqual({r.version for r in self.book.rows.values()}, {0})

    def test_unknown_leg_reports_and_changes_nothing(self):
        with self.assertRaises(KeyError):
            self.service.book("B-6", legs(("leg-zzz", "acc-1", 1)), "run")
        self.assertEqual(self.events.all_streams(), [])
        self.assertEqual(self.ledger.entries(), ())


if __name__ == "__main__":
    unittest.main()
