import threading
import unittest
from decimal import Decimal

from coldledger.storage.event_store import EventStore
from coldledger.transport.capacity_commit import Book, Capacity, commit_booking
from coldledger.transport.carrier_capacity import CarrierPool, LaneNeed, plan
from coldledger.transport.container_ledger import ContainerLedger

D = Decimal
LEGS = ('L1', 'L2', 'L3')


def make_book():
    return Book([Capacity('L1', D('10')), Capacity('L2', D('5')), Capacity('L3', D('3'))])


def versions(book, ids=LEGS):
    return {leg: row.version for leg, row in book.snapshot(ids).items()}


def available(book, ids=LEGS):
    return {leg: row.available for leg, row in book.snapshot(ids).items()}


class BookingCommitTests(unittest.TestCase):
    def test_last_leg_shortage_leaves_no_partial_occupation(self):
        book, ledger, events = make_book(), ContainerLedger(), EventStore()
        requests = {'L1': D('4'), 'L2': D('4'), 'L3': D('4')}  # L3 only holds 3
        with self.assertRaises(ValueError):
            commit_booking(book, ledger, events, 'B1', requests, versions(book), 'acct')
        self.assertEqual(available(book), {'L1': D('10'), 'L2': D('5'), 'L3': D('3')})
        self.assertEqual(versions(book), {'L1': 0, 'L2': 0, 'L3': 0})
        self.assertEqual(ledger.entries(), ())
        self.assertEqual(events.read('booking:B1'), [])

    def test_version_conflict_leaves_no_intermediate_state(self):
        book, ledger, events = make_book(), ContainerLedger(), EventStore()
        commit_booking(book, ledger, events, 'B0', {'L2': D('2')}, {'L2': 0}, 'acct')
        stale = {'L1': 0, 'L2': 0, 'L3': 0}  # L2 has since moved to version 1
        with self.assertRaises(RuntimeError):
            commit_booking(book, ledger, events, 'B1',
                           {'L1': D('1'), 'L2': D('1'), 'L3': D('1')}, stale, 'acct')
        self.assertEqual(available(book), {'L1': D('10'), 'L2': D('3'), 'L3': D('3')})
        self.assertEqual(ledger.balance('acct'), D('2'))  # only B0's posting
        self.assertEqual([e.reference for e in ledger.entries('acct')], ['B0:L2'])
        self.assertEqual(events.read('booking:B1'), [])

    def test_success_commits_capacity_ledger_and_event_together(self):
        book, ledger, events = make_book(), ContainerLedger(), EventStore()
        receipt = commit_booking(book, ledger, events, 'B1',
                                 {'L1': D('4'), 'L2': D('2'), 'L3': D('1')},
                                 versions(book), 'acct')
        self.assertEqual(available(book), {'L1': D('6'), 'L2': D('3'), 'L3': D('2')})
        self.assertEqual(versions(book), {'L1': 1, 'L2': 1, 'L3': 1})
        self.assertEqual(ledger.balance('acct'), D('7'))
        self.assertEqual({e.reference for e in ledger.entries('acct')},
                         {'B1:L1', 'B1:L2', 'B1:L3'})
        stream = events.read('booking:B1')
        self.assertEqual(len(stream), 1)
        self.assertEqual(stream[0].kind, 'booking_committed')
        self.assertEqual(stream[0].sequence, 1)
        self.assertEqual(stream[0].payload['booking_id'], 'B1')
        self.assertEqual(stream[0].payload['versions'], {'L1': 1, 'L2': 1, 'L3': 1})
        self.assertEqual(receipt.event.sequence, 1)
        self.assertEqual(len(receipt.entries), 3)

    def test_event_append_failure_triggers_compensating_release(self):
        book, ledger, events = make_book(), ContainerLedger(), EventStore()

        def boom(stream, expected, kind, payload):
            raise RuntimeError('stream conflict')

        events.append = boom
        with self.assertRaises(RuntimeError):
            commit_booking(book, ledger, events, 'B1',
                           {'L1': D('4'), 'L2': D('2')}, versions(book, ('L1', 'L2')), 'acct')
        # capacity fully restored, ledger nets to zero with an auditable
        # post/reversal trail, and no commit event was recorded
        self.assertEqual(available(book, ('L1', 'L2')), {'L1': D('10'), 'L2': D('5')})
        self.assertEqual(ledger.balance('acct'), D('0'))
        entries = ledger.entries('acct')
        self.assertEqual(len(entries), 4)
        self.assertEqual(sorted(e.reason for e in entries),
                         ['booking', 'booking', 'booking_compensation', 'booking_compensation'])
        self.assertEqual(sum(1 for e in entries if e.reversal_of is not None), 2)
        ledger.audit()

    def test_duplicate_booking_id_rejected_before_reserving(self):
        book, ledger, events = make_book(), ContainerLedger(), EventStore()
        commit_booking(book, ledger, events, 'B1', {'L1': D('4')}, {'L1': 0}, 'acct')
        with self.assertRaises(ValueError):
            commit_booking(book, ledger, events, 'B1', {'L1': D('4')}, versions(book, ('L1',)), 'acct')
        self.assertEqual(available(book, ('L1',)), {'L1': D('6')})
        self.assertEqual(ledger.balance('acct'), D('4'))

    def test_container_balance_counts_debits_and_reversals(self):
        ledger = ContainerLedger()
        entry = ledger.post('a', 'r1', D('10'), 'booking')
        ledger.reverse(entry.sequence, 'r1:undo', 'booking_compensation')
        self.assertEqual(ledger.balance('a'), D('0'))
        ledger.post('a', 'r2', D('5'), 'booking')
        ledger.transfer('a', 'b', D('5'), 't1', 'reposition')
        self.assertEqual(ledger.balance('a'), D('0'))
        self.assertEqual(ledger.balance('b'), D('5'))
        with self.assertRaises(ValueError):
            ledger.transfer('a', 'b', D('1'), 't2', 'reposition')

    def test_carrier_plan_prefers_same_group(self):
        needs = [LaneNeed('d1', D('5'), 1, 'reefer')]
        pools = [CarrierPool('p1', D('10'), 'reefer'), CarrierPool('p2', D('10'), 'ambient')]
        allocation = plan(needs, pools)
        self.assertEqual([(a.demand_key, a.resource_key, a.quantity) for a in allocation],
                         [('d1', 'p1', D('5'))])
        with self.assertRaises(ValueError):
            plan(needs, [CarrierPool('p2', D('10'), 'ambient')])

    def test_concurrent_bookings_never_overcommit(self):
        book = Book([Capacity('L1', D('50')), Capacity('L2', D('50'))])
        ledger, events = ContainerLedger(), EventStore()
        successes, lock = [], threading.Lock()

        def worker(wid):
            for i in range(10):
                booking_id = f'W{wid}:{i}'
                while True:
                    try:
                        commit_booking(book, ledger, events, booking_id,
                                       {'L1': 1, 'L2': 1}, versions(book, ('L1', 'L2')), 'acct')
                        with lock:
                            successes.append(booking_id)
                        break
                    except RuntimeError:
                        continue  # lost a version race: reload and retry
                    except ValueError:
                        return  # capacity exhausted

        threads = [threading.Thread(target=worker, args=(w,)) for w in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        held = available(book, ('L1', 'L2'))
        self.assertEqual(len(successes), 50)
        self.assertEqual(held, {'L1': D('0'), 'L2': D('0')})
        self.assertGreaterEqual(held['L1'], D('0'))
        self.assertEqual(ledger.balance('acct'), D('100'))
        self.assertEqual(len(events.all_streams()), 50)
        ledger.audit()


if __name__ == '__main__':
    unittest.main()
