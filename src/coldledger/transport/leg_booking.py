"""Atomic multi-leg transport booking.

A booking spans several legs and three independent stores:

* ``Book`` -- per-leg carrier capacity (optimistically versioned),
* ``ContainerLedger`` -- the container occupancy account entries,
* ``EventStore`` -- the recorded booking event.

The strategy is fixed as *prevalidate, then commit* -- never
"reserve now and compensate later": a compensating release can itself
fail or race, which is exactly how unexplained half-occupancy is left
behind when, for example, the final leg is full.

``book`` therefore works like an optimistic-concurrency transaction:

1. **Prepare** -- read the current leg versions and the event-stream
   length without locking.
2. **Validate + commit** -- acquire every store lock in one fixed
   global order (capacity -> ledger -> events, the same order in every
   process, so the system cannot deadlock), re-read and re-validate
   *all* preconditions under those locks, and only then apply every
   mutation.  Because nothing is mutated before every check has passed,
   a failure -- including a version that drifted because of a competing
   booking -- raises before any store changes state.  No intermediate
   state can ever be observed, and no release/compensation is needed.

A stale read from step 1 raises :class:`BookingConflict`; ``book``
retries the prepare/commit cycle transparently.  Capacity and input
errors are permanent and propagate immediately.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from .capacity_commit import Book, Capacity
from .container_ledger import ContainerLedger, Entry
from ..storage.event_store import EventStore, Event

BOOKING_KIND = "multi_leg_booked"
_MAX_ATTEMPTS = 1000


class BookingConflict(RuntimeError):
    """Raised inside a commit attempt when a read version has drifted.

    The conflict is raised *before* any mutation, so it is always safe
    to retry the whole booking from a fresh prepare.
    """


@dataclass(frozen=True)
class LegRequest:
    leg_id: str
    account: str
    quantity: Decimal


@dataclass(frozen=True)
class BookingResult:
    reference: str
    legs: tuple[Capacity, ...]
    entries: tuple[Entry, ...]
    event: Event

    @property
    def leg_ids(self) -> tuple[str, ...]:
        return tuple(r.leg_id for r in self.legs)


class MultiLegBooking:
    """Commits multi-leg bookings across capacity, ledger and events atomically."""

    def __init__(self, capacity: Book, ledger: ContainerLedger, events: EventStore):
        self.capacity = capacity
        self.ledger = ledger
        self.events = events
        self._completed: dict[str, BookingResult] = {}

    def outcome(self, reference: str) -> BookingResult | None:
        return self._completed.get(reference)

    def book(self, reference: str, legs: list[LegRequest], reason: str) -> BookingResult:
        """Reserve every leg, post every container entry, record one event.

        Either all three stores commit together or none of them change.
        A repeated ``reference`` returns the original result without
        re-applying anything (idempotent retry).
        """
        reqs = self._normalise(reference, legs, reason)
        stream = self._stream(reference)
        for _ in range(_MAX_ATTEMPTS):
            # Prepare: optimistic reads outside the locks. An unknown
            # leg raises KeyError here; it is permanent, not retried.
            snap = self.capacity.snapshot(list(reqs))
            versions = {leg_id: snap[leg_id].version for leg_id in reqs}
            expected_stream = self.events.stream_version(stream)
            try:
                return self._commit(reference, reqs, reason, stream, versions, expected_stream)
            except BookingConflict:
                continue
        raise BookingConflict(f"booking {reference} kept conflicting")

    # -- internals ---------------------------------------------------------

    def _commit(self, reference, reqs, reason, stream, prepared_versions, expected_stream):
        # Fixed global lock order: capacity -> ledger -> events.
        with self.capacity.lock, self.ledger.lock, self.events.lock:
            # Idempotency is authoritative here, under all locks.
            done = self._completed.get(reference)
            if done is not None:
                return done

            # ---- validate phase: no mutation until every check passes ----
            if self.events.stream_version(stream) != expected_stream:
                raise BookingConflict(f"event stream {stream} changed during booking")
            # A committed event for this reference means the booking already
            # happened (possibly through another orchestrator instance): that
            # is permanent, not a conflict worth retrying.
            if any(e.kind == BOOKING_KIND for e in self.events.read(stream)):
                raise ValueError(f"duplicate booking reference {reference}")

            rows = self.capacity.rows
            current: dict[str, Capacity] = {}
            for leg_id, (account, q) in reqs.items():
                row = rows.get(leg_id)
                if row is None:
                    raise KeyError(leg_id)
                if prepared_versions[leg_id] != row.version:
                    raise BookingConflict(f"leg {leg_id} version changed during booking")
                if row.available < q:
                    raise ValueError(f"insufficient capacity on leg {leg_id}")
                current[leg_id] = row
                leg_ref = self._leg_reference(reference, leg_id)
                if self.ledger.has_reference(account, leg_ref):
                    raise BookingConflict(f"ledger reference for leg {leg_id} appeared")

            # ---- apply phase: every precondition held under the locks -----
            capacities = self.capacity.reserve_atomic(
                {leg_id: q for leg_id, (_, q) in reqs.items()},
                {leg_id: current[leg_id].version for leg_id in reqs},
            )
            entries: list[Entry] = []
            for leg_id, (account, q) in reqs.items():
                entries.append(
                    self.ledger.post(
                        account,
                        self._leg_reference(reference, leg_id),
                        q,
                        reason,
                    )
                )
            payload = {
                "reference": reference,
                "reason": reason,
                "legs": [
                    {
                        "leg_id": leg_id,
                        "account": account,
                        "quantity": str(q),
                        "capacity_version": capacities[leg_id].version,
                    }
                    for leg_id, (account, q) in reqs.items()
                ],
            }
            event = self.events.append(stream, expected_stream, BOOKING_KIND, payload)

            result = BookingResult(
                reference,
                tuple(capacities[leg_id] for leg_id in reqs),
                tuple(entries),
                event,
            )
            self._completed[reference] = result
            return result

    @staticmethod
    def _normalise(reference, legs, reason) -> dict[str, tuple[str, Decimal]]:
        if not reference:
            raise ValueError("booking reference required")
        if not reason:
            raise ValueError("reason required")
        if not legs:
            raise ValueError("at least one leg required")
        reqs: dict[str, tuple[str, Decimal]] = {}
        for leg in legs:
            q = Decimal(str(leg.quantity))
            if q <= 0:
                raise ValueError(f"positive quantity required on leg {leg.leg_id}")
            if not leg.account:
                raise ValueError(f"container account required on leg {leg.leg_id}")
            if leg.leg_id in reqs:
                raise ValueError(f"duplicate leg {leg.leg_id} in booking {reference}")
            reqs[leg.leg_id] = (leg.account, q)
        return reqs

    @staticmethod
    def _stream(reference: str) -> str:
        return f"booking:{reference}"

    @staticmethod
    def _leg_reference(reference: str, leg_id: str) -> str:
        return f"{reference}:leg:{leg_id}"
