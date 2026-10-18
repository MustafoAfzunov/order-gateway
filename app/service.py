"""OrderService: turns a signal into at most one broker order."""

import json
import logging
import sqlite3
import threading
from collections import Counter
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

from app.broker import BrokerOrder, BrokerStatus, BrokerTimeout, MockBroker
from app.config import Settings
from app.db import connection
from app.logging_setup import LOGGER_NAME, log_event
from app.models import TERMINAL, BrokerUpdate, Order, OrderStatus, Signal, can_transition
from app.risk import is_kill_switch_on, max_value_violation, set_kill_switch

log = logging.getLogger(LOGGER_NAME)

S = OrderStatus

BROKER_TO_ORDER_STATUS = {
    BrokerStatus.ACCEPTED: S.ACCEPTED,
    BrokerStatus.REJECTED: S.REJECTED,
    BrokerStatus.PARTIALLY_FILLED: S.PARTIALLY_FILLED,
    BrokerStatus.FILLED: S.FILLED,
}

# Columns _transition may set. Names are put into the SQL text, so they must never come
# from outside the code.
_UPDATABLE = {"filled_qty", "last_seq", "broker_order_id", "reject_reason"}


class SignalConflict(Exception):
    """Same signal_id, different contents. We refuse rather than guess which one is right."""


class InvalidTransition(Exception):
    pass


class IgnoreReason(StrEnum):
    STALE_OR_DUPLICATE = "STALE_OR_DUPLICATE"  # seq <= last_seq
    ORDER_TERMINAL = "ORDER_TERMINAL"
    INVALID_TRANSITION = "INVALID_TRANSITION"
    FILL_DECREASED = "FILL_DECREASED"
    FILL_EXCEEDS_QTY = "FILL_EXCEEDS_QTY"
    FILL_INCONSISTENT_WITH_STATUS = "FILL_INCONSISTENT_WITH_STATUS"
    UNKNOWN_ORDER = "UNKNOWN_ORDER"


@dataclass(frozen=True)
class UpdateResult:
    applied: bool
    reason: IgnoreReason | None  # None when applied
    order: Order | None  # state after handling; None for an unknown order


# Everything that isn't terminal: the broker may know something about these that we don't.
UNRESOLVED = (S.NEW, S.SUBMITTING, S.UNKNOWN, S.ACCEPTED, S.PARTIALLY_FILLED)


@dataclass(frozen=True)
class ReconcileResult:
    signal_id: str
    status_before: OrderStatus
    status_after: OrderStatus
    note: str


def _fill_matches_status(status: OrderStatus, filled: Decimal, qty: Decimal) -> bool:
    if status is S.FILLED:
        return filled == qty
    if status is S.PARTIALLY_FILLED:
        return Decimal(0) < filled < qty
    return filled == 0  # ACCEPTED, REJECTED


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


class OrderService:
    def __init__(
        self,
        db_path: str,
        broker: MockBroker,
        settings: Settings,
        now: Callable[[], str] = utc_now,
    ) -> None:
        self.db_path = db_path
        self.broker = broker
        self.settings = settings
        self.now = now
        # client_order_ids this process is claiming/sending right now. A SUBMITTING order that
        # is NOT in here was left behind by a crash, and reconcile() may recover it. A Counter,
        # not a set: a thread that loses the claim must not erase the winner's mark.
        # In-memory on purpose: after a restart it's empty, so every SUBMITTING row is
        # (correctly) treated as orphaned. Assumes one gateway process per DB file.
        self._in_flight: Counter[str] = Counter()
        self._in_flight_lock = threading.Lock()

    def submit(self, signal: Signal) -> Order:
        with connection(self.db_path) as conn:
            # 1. Record the signal. UNIQUE(signal_id) + DO NOTHING means concurrent duplicates
            #    all end up looking at the same single row.
            inserted = self._insert(conn, signal)
            order = self._get(conn, signal.signal_id)
            if not inserted:
                try:
                    self._check_same_payload(order, signal)
                except SignalConflict:
                    self._log_line(order, "SIGNAL_CONFLICT", logging.WARNING,
                              qty=signal.qty, reference_price=signal.reference_price,
                              side=signal.side.value, instrument=signal.instrument)
                    raise
                self._log_line(order, "DUPLICATE", status=order.status.value)
            if order.status is not S.NEW:
                return order  # duplicate of a signal that is already being handled

            # 2. Kill switch. Stored as BLOCKED (not just refused) so there is an audit trail
            #    and a resend gets the same answer.
            if is_kill_switch_on(conn):
                self._transition(conn, order, S.BLOCKED, "BLOCKED_KILL_SWITCH",
                                 reject_reason="kill switch is on")
                return self._get(conn, signal.signal_id)

            # 3. Max order value.
            reason = max_value_violation(signal.qty, signal.reference_price,
                                         self.settings.max_order_value)
            if reason:
                self._transition(conn, order, S.REJECTED_RISK, "REJECTED_RISK",
                                 reject_reason=reason)
                return self._get(conn, signal.signal_id)

            # 4. Atomic claim. Only the caller whose UPDATE ... WHERE status='NEW' changed a row
            #    may call the broker; everyone else returns what's stored. This also covers a
            #    duplicate that raced past steps 2-3: it loses here.
            #    Write-ahead: the claim is committed before the broker call below, so if we
            #    crash during the call, the DB says SUBMITTING and recovery knows to ask the
            #    broker. It never looks like an order we didn't send.
            with self._sending(order.client_order_id):
                if not self._transition(conn, order, S.SUBMITTING, "CLAIMED",
                                        count_attempt=True):
                    return self._get(conn, signal.signal_id)

                # 5. Call the broker. On a timeout the order is now UNKNOWN, and we ask the
                #    broker what happened before doing anything else.
                order = self._send(conn, self._get(conn, signal.signal_id))
        if order.status is S.UNKNOWN:
            return self.resolve_unknown(order)
        return order

    def resolve_unknown(self, order: Order) -> Order:
        """Find out what happened to an order whose broker call timed out.

        Why we never resend blindly: a timeout means "no answer", not "no order". The broker
        may have accepted (or even filled) the order and only the response got lost. Sending
        again without looking first would, with a fresh ID, create a second real order, and
        even with the same ID it would leave us guessing at the broker's state. So we always
        ask first with get_order(client_order_id). Because client_order_id is deterministic,
        that question has a definite answer.
        """
        with connection(self.db_path) as conn:
            order = self._get(conn, order.signal_id)  # re-read: someone may have resolved it
            if order.status is not S.UNKNOWN:
                return order

            try:
                broker_order = self.broker.get_order(order.client_order_id)
            except BrokerTimeout:
                # Even the lookup failed. Stay UNKNOWN; reconciliation will ask again later.
                self._record_event(conn, order.id, order.signal_id, order.client_order_id,
                                "BROKER_LOOKUP_FAILED", S.UNKNOWN, S.UNKNOWN, None)
                return order

            self._log_lookup(order, broker_order)
            if broker_order is not None:
                # The broker has it: its state is the truth, adopt it (same rules as a push).
                self._apply_broker_snapshot(broker_order, "RESOLVED_FROM_BROKER", "get_order")
                return self._get(conn, order.signal_id)

            # The broker has never seen this client_order_id, so a resend can't duplicate
            # anything. It's still a new order going to market, so the kill switch applies.
            if is_kill_switch_on(conn):
                # Stays UNKNOWN (not BLOCKED): a request still in flight could reach the
                # broker later, so "not found" now doesn't prove the order never exists.
                self._record_event(conn, order.id, order.signal_id, order.client_order_id,
                                "RETRY_SKIPPED_KILL_SWITCH", S.UNKNOWN, S.UNKNOWN, None)
                return order

            # Re-claim UNKNOWN -> SUBMITTING, so that if two callers resolve the same order at
            # once, only one of them resends.
            with self._sending(order.client_order_id):
                if not self._transition(conn, order, S.SUBMITTING, "RETRY_CLAIMED",
                                        count_attempt=True):
                    return self._get(conn, order.signal_id)

                # Same client_order_id as the first attempt. If that request does turn up at
                # the broker after all, its idempotency on client_order_id still keeps it to
                # one order. One retry per call: if this one times out too, we return UNKNOWN
                # rather than loop. The caller (or reconciliation) decides when to ask again.
                return self._send(conn, self._get(conn, order.signal_id))

    def handle_update(self, update: BrokerUpdate | dict[str, Any]) -> UpdateResult:
        """Apply one broker push update, or ignore it with a recorded reason.

        Broker feeds can replay updates and deliver them out of order. Every check below
        compares against what's stored, so delivering the same update twice, or an old one
        late, leaves the order exactly as it was.
        """
        if not isinstance(update, BrokerUpdate):
            update = BrokerUpdate.model_validate(update)  # malformed input raises here
        return self._apply_broker_info(update, "BROKER_UPDATE_APPLIED", "push")

    def _apply_broker_info(
        self, update: BrokerUpdate, applied_event: str, source: str
    ) -> UpdateResult:
        """The single path for every piece of broker information: pushed updates, the
        place_order response, get_order lookups and reconciliation. So the seq, terminal and
        fill rules apply the same way everywhere."""
        with connection(self.db_path) as conn:
            # BEGIN IMMEDIATE takes the write lock *before* we read. The read, the checks and
            # the write then act on a row nobody else can change in between. With a plain
            # BEGIN, two threads could both read last_seq=1 and both apply seq 2.
            with self._tx(conn) as pending:
                result = self._apply_update(conn, update, applied_event, source, pending)
        return result

    def reconcile(self) -> list[ReconcileResult]:
        """Compare every unresolved order with the broker and adopt the broker's truth.

        Run at startup (recovers what a crash left behind) and periodically (catches missed
        push updates). It only reads from the broker, through the same update path as pushes.
        The one exception is an order the broker has never seen, which is resent, so the kill
        switch blocks only that step and everything else keeps working.
        """
        with connection(self.db_path) as conn:
            placeholders = ", ".join("?" for _ in UNRESOLVED)
            rows = conn.execute(
                f"SELECT * FROM orders WHERE status IN ({placeholders}) ORDER BY id",
                [st.value for st in UNRESOLVED],
            ).fetchall()

        results = []
        for order in (Order.from_row(r) for r in rows):
            try:
                note = self._reconcile_one(order)
            except Exception as e:  # one bad order must not stop the others being reconciled
                log_event(log, "RECONCILE_ERROR", signal_id=order.signal_id,
                          client_order_id=order.client_order_id, level=logging.ERROR,
                          exc_info=True, error=repr(e))
                note = f"error: {e!r}"
            after = self.get(order.signal_id)
            result = ReconcileResult(order.signal_id, order.status,
                                     after.status if after else order.status, note)
            self._log_line(order, "RECONCILE_RESULT", status_before=result.status_before.value,
                      status_after=result.status_after.value, note=note)
            results.append(result)
        log_event(log, "RECONCILE_DONE", signal_id=None, client_order_id=None,
                  checked=len(results),
                  changed=sum(r.status_before != r.status_after for r in results))
        return results

    def _reconcile_one(self, order: Order) -> str:
        # Re-read: the list was loaded before the loop and may be stale. The read must also
        # come BEFORE the in-flight check below. A sender marks itself in flight before it
        # claims, so a claim we can see here is always one whose sender we can also see.
        order = self.get(order.signal_id) or order
        if order.status in TERMINAL:
            return "already resolved"
        if order.status is S.SUBMITTING:
            if self._is_in_flight(order.client_order_id):
                # This process is sending it right now. Its own response will settle it,
                # and resending here could put a second request on the wire.
                return "skipped: in flight"
            # Nobody here is sending it, so a crash interrupted the broker call. The request
            # may or may not have arrived. That's exactly what UNKNOWN means.
            with connection(self.db_path) as conn:
                orphaned = self._transition(conn, order, S.UNKNOWN, "ORPHANED_SUBMITTING")
            order = self.get(order.signal_id) or order
            if not orphaned and order.status is not S.UNKNOWN:
                # Someone else got there first (e.g. re-claimed it and is sending).
                # Leave it to them.
                return f"changed concurrently -> {order.status}"

        if order.status is S.UNKNOWN:
            # get_order first; resend only if not found and the kill switch is off.
            return f"resolved unknown -> {self.resolve_unknown(order).status}"

        try:
            broker_order = self.broker.get_order(order.client_order_id)
        except BrokerTimeout:
            with connection(self.db_path) as conn:
                self._record_event(conn, order.id, order.signal_id, order.client_order_id,
                                "BROKER_LOOKUP_FAILED", order.status, order.status, None)
            return "lookup failed"

        self._log_lookup(order, broker_order)
        if broker_order is None:
            if order.status is S.NEW:
                # Expected: NEW means we never called the broker. Reconcile doesn't send it;
                # a resubmission of the signal resumes it through submit() and its checks.
                return "not at broker (never sent)"
            # We hold a confirmed order the broker no longer knows. That should not happen, and
            # guessing is worse than flagging it, so record it and leave the order as it is.
            with connection(self.db_path) as conn:
                self._record_event(conn, order.id, order.signal_id, order.client_order_id,
                                "RECONCILE_MISSING_AT_BROKER", order.status, order.status, None)
            return "missing at broker"

        if broker_order.seq == order.last_seq:
            # Nothing new. Return without an event, so a periodic reconcile doesn't fill
            # order_events with "stale" rows for every open order.
            return "in sync"
        # Newer (we missed a push), or older than ours: an anomaly, ignored and logged by the
        # update path.
        result = self._apply_broker_snapshot(broker_order, "RECONCILED", "reconcile")
        if result.applied:
            return "updated from broker"
        return f"broker snapshot ignored: {result.reason}"

    def set_kill_switch(self, on: bool) -> bool:
        with connection(self.db_path) as conn:
            was_on = is_kill_switch_on(conn)
            set_kill_switch(conn, on)
        log_event(log, "KILL_SWITCH_SET", signal_id=None, client_order_id=None,
                  level=logging.WARNING, on=on, was_on=was_on)
        return on

    def get(self, signal_id: str) -> Order | None:
        with connection(self.db_path) as conn:
            row = conn.execute("SELECT * FROM orders WHERE signal_id = ?", (signal_id,)).fetchone()
        return Order.from_row(row) if row else None

    def events(self, signal_id: str) -> list[dict[str, Any]]:
        with connection(self.db_path) as conn:
            rows = conn.execute(
                "SELECT e.* FROM order_events e JOIN orders o ON o.id = e.order_id"
                " WHERE o.signal_id = ? ORDER BY e.id",
                (signal_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    # --- internals -------------------------------------------------------------------------

    def _log_lookup(self, order: Order, broker_order: BrokerOrder | None) -> None:
        if broker_order is None:
            self._log_line(order, "BROKER_LOOKUP", result="not_found",
                           our_status=order.status.value)
        else:
            self._log_line(order, "BROKER_LOOKUP", result="found", our_status=order.status.value,
                      broker_status=broker_order.status.value, seq=broker_order.seq,
                      filled_qty=broker_order.filled_qty)

    def _send(self, conn: sqlite3.Connection, order: Order) -> Order:
        """Call place_order for an order we have claimed (status SUBMITTING). The claim is
        already committed (write-ahead) and no transaction is open, so we hold no DB lock
        while waiting on the network."""
        self._log_line(order, "BROKER_SEND", attempt=order.submit_attempts)
        try:
            broker_order = self.broker.place_order(
                order.client_order_id, order.instrument, order.side.value,
                order.qty, order.reference_price,
            )
        except BrokerTimeout:
            self._transition(conn, order, S.UNKNOWN, "BROKER_TIMEOUT")
            return self._get(conn, order.signal_id)
        self._apply_broker_snapshot(broker_order, "BROKER_RESPONSE", "place_order")
        return self._get(conn, order.signal_id)

    def _apply_broker_snapshot(
        self, broker_order: BrokerOrder, event: str, source: str
    ) -> UpdateResult:
        """Feed a broker snapshot (from place_order or get_order) through the update path.
        A snapshot is just an update we asked for: same seq, same cumulative fill."""
        return self._apply_broker_info(BrokerUpdate(
            client_order_id=broker_order.client_order_id,
            broker_order_id=broker_order.broker_order_id,
            seq=broker_order.seq,
            status=BROKER_TO_ORDER_STATUS[broker_order.status],
            cumulative_filled_qty=broker_order.filled_qty,
        ), event, source)

    @contextmanager
    def _sending(self, client_order_id: str) -> Iterator[None]:
        with self._in_flight_lock:
            self._in_flight[client_order_id] += 1
        try:
            yield
        finally:
            with self._in_flight_lock:
                self._in_flight[client_order_id] -= 1
                if self._in_flight[client_order_id] == 0:
                    del self._in_flight[client_order_id]

    def _is_in_flight(self, client_order_id: str) -> bool:
        with self._in_flight_lock:
            return client_order_id in self._in_flight

    def _apply_update(
        self,
        conn: sqlite3.Connection,
        u: BrokerUpdate,
        applied_event: str,
        source: str,
        pending: list[dict[str, Any]],
    ) -> UpdateResult:
        row = conn.execute(
            "SELECT * FROM orders WHERE client_order_id = ?", (u.client_order_id,)
        ).fetchone()
        if row is None:
            # Not ours (we always insert before we send, so a real order of ours has a row).
            # Nothing to change. order_events needs an order row, so this goes to the log only.
            log_event(log, "BROKER_UPDATE_IGNORED", signal_id=None,
                      client_order_id=u.client_order_id, level=logging.WARNING,
                      reason=IgnoreReason.UNKNOWN_ORDER.value, source=source, seq=u.seq,
                      status=u.status.value, cumulative_filled_qty=u.cumulative_filled_qty)
            return UpdateResult(False, IgnoreReason.UNKNOWN_ORDER, None)

        order = Order.from_row(row)
        new = u.status
        reason = self._why_ignore(order, u)
        detail = {"source": source, "seq": u.seq, "status": new.value,
                  "cumulative_filled_qty": str(u.cumulative_filled_qty)}

        if reason is not None:
            self._record_event(conn, order.id, order.signal_id, order.client_order_id,
                            "BROKER_UPDATE_IGNORED", order.status, order.status,
                            {**detail, "reason": reason.value, "last_seq": order.last_seq,
                             "filled_qty": str(order.filled_qty)}, pending)
            return UpdateResult(False, reason, order)

        extra = ", reject_reason = 'rejected by broker'" if new is S.REJECTED else ""
        cur = conn.execute(
            "UPDATE orders SET status = ?, filled_qty = ?, last_seq = ?,"
            " broker_order_id = COALESCE(broker_order_id, ?), updated_at = ?" + extra +
            # Redundant under the write lock, but it keeps the SQL itself unable to move
            # last_seq backwards or touch a terminal order.
            " WHERE id = ? AND last_seq < ? AND status NOT IN"
            " ('FILLED', 'REJECTED', 'BLOCKED', 'REJECTED_RISK')",
            (new.value, str(u.cumulative_filled_qty), u.seq, u.broker_order_id, self.now(),
             order.id, u.seq),
        )
        if cur.rowcount != 1:  # can't happen while we hold the write lock; fail loudly if it does
            raise RuntimeError(f"update for {u.client_order_id} passed checks but changed no row")
        self._record_event(conn, order.id, order.signal_id, order.client_order_id,
                        applied_event, order.status, new, detail, pending)
        return UpdateResult(True, None, Order.from_row(conn.execute(
            "SELECT * FROM orders WHERE id = ?", (order.id,)).fetchone()))

    @staticmethod
    def _why_ignore(order: Order, u: BrokerUpdate) -> "IgnoreReason | None":
        """The order of checks only decides which reason gets logged. Any failure means
        the update is ignored."""
        if u.seq <= order.last_seq:
            return IgnoreReason.STALE_OR_DUPLICATE
        if order.status in TERMINAL:
            return IgnoreReason.ORDER_TERMINAL
        if not can_transition(order.status, u.status):
            return IgnoreReason.INVALID_TRANSITION
        if u.cumulative_filled_qty < order.filled_qty:
            return IgnoreReason.FILL_DECREASED
        if u.cumulative_filled_qty > order.qty:
            return IgnoreReason.FILL_EXCEEDS_QTY
        if not _fill_matches_status(u.status, u.cumulative_filled_qty, order.qty):
            return IgnoreReason.FILL_INCONSISTENT_WITH_STATUS
        return None

    def _insert(self, conn: sqlite3.Connection, signal: Signal) -> bool:
        now = self.now()
        with self._tx(conn) as pending:
            cur = conn.execute(
                "INSERT INTO orders (signal_id, client_order_id, instrument, side, qty,"
                " reference_price, status, created_at, updated_at)"
                " VALUES (?, ?, ?, ?, ?, ?, 'NEW', ?, ?)"
                " ON CONFLICT(signal_id) DO NOTHING",
                (signal.signal_id, signal.client_order_id, signal.instrument, signal.side.value,
                 str(signal.qty), str(signal.reference_price), now, now),
            )
            inserted = cur.rowcount == 1
            if inserted:
                self._record_event(conn, cur.lastrowid, signal.signal_id, signal.client_order_id,
                                "RECEIVED", None, S.NEW,
                                {"instrument": signal.instrument, "side": signal.side.value,
                                 "qty": str(signal.qty),
                                 "reference_price": str(signal.reference_price)},
                                pending)
        return inserted

    def _transition(
        self,
        conn: sqlite3.Connection,
        order: Order,
        new: OrderStatus,
        event: str,
        count_attempt: bool = False,
        **fields: Any,
    ) -> bool:
        """Compare-and-set: move the order from the status we read to `new`, and log the event,
        in one transaction. Returns False if someone else changed the status first."""
        if not can_transition(order.status, new):
            raise InvalidTransition(f"{order.status} -> {new}")
        if not set(fields) <= _UPDATABLE:
            raise ValueError(f"not updatable: {set(fields) - _UPDATABLE}")

        assignments = ["status = ?", "updated_at = ?"] + [f"{k} = ?" for k in fields]
        if count_attempt:
            assignments.append("submit_attempts = submit_attempts + 1")
        params = [new.value, self.now(), *fields.values(), order.id, order.status.value,
                  order.submit_attempts]

        with self._tx(conn) as pending:
            # Compare-and-set: the update only applies if the order is still in the state we
            # based our decision on. Status alone isn't enough, because SUBMITTING can come back:
            # SUBMITTING -> UNKNOWN -> SUBMITTING (retry). A caller holding the old SUBMITTING
            # would match the new one and could trigger a second send (the ABA problem). Every
            # claim increments submit_attempts, so (status, submit_attempts) identifies one
            # specific attempt.
            cur = conn.execute(
                f"UPDATE orders SET {', '.join(assignments)}"
                " WHERE id = ? AND status = ? AND submit_attempts = ?",
                params,
            )
            changed = cur.rowcount == 1
            if changed:
                self._record_event(conn, order.id, order.signal_id, order.client_order_id, event,
                                order.status, new, fields or None, pending)
        return changed

    def _record_event(
        self,
        conn: sqlite3.Connection,
        order_id: int | None,
        signal_id: str,
        client_order_id: str,
        event: str,
        from_status: OrderStatus | None,
        to_status: OrderStatus | None,
        detail: dict[str, Any] | None,
        pending: list[dict[str, Any]] | None = None,
    ) -> None:
        """Write the per-order audit row and the matching log line. Inside a transaction, pass
        `pending`: the line is then logged only after COMMIT, so the log never reports a change
        that was rolled back."""
        conn.execute(
            "INSERT INTO order_events (order_id, event, from_status, to_status, detail,"
            " created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (order_id, event, from_status, to_status,
             json.dumps(detail) if detail else None, self.now()),
        )
        line = {"event": event, "signal_id": signal_id, "client_order_id": client_order_id,
                "from_status": from_status, "to_status": to_status, **(detail or {})}
        if pending is None:
            self._write_log_line(line)
        else:
            pending.append(line)

    @staticmethod
    def _write_log_line(line: dict[str, Any]) -> None:
        line = dict(line)
        log_event(log, line.pop("event"), signal_id=line.pop("signal_id"),
                  client_order_id=line.pop("client_order_id"), **line)

    @staticmethod
    def _log_line(order: Order, event: str, level: int = logging.INFO, **fields: Any) -> None:
        """Log-only line (no order_events row), for things that don't change the order."""
        log_event(log, event, signal_id=order.signal_id, client_order_id=order.client_order_id,
                  level=level, **fields)

    @contextmanager
    def _tx(self, conn: sqlite3.Connection) -> Iterator[list[dict[str, Any]]]:
        """BEGIN IMMEDIATE ... COMMIT (ROLLBACK on any error). Yields a list for log lines that
        must wait until the commit has succeeded."""
        pending: list[dict[str, Any]] = []
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield pending
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        for line in pending:
            self._write_log_line(line)

    def _get(self, conn: sqlite3.Connection, signal_id: str) -> Order:
        row = conn.execute("SELECT * FROM orders WHERE signal_id = ?", (signal_id,)).fetchone()
        return Order.from_row(row)

    @staticmethod
    def _check_same_payload(order: Order, signal: Signal) -> None:
        # Compare as Decimals, so "10" and "10.0" count as the same quantity.
        same = (order.instrument, order.side, order.qty, order.reference_price) == (
            signal.instrument, signal.side, signal.qty, signal.reference_price)
        if not same:
            raise SignalConflict(f"signal_id {signal.signal_id} already exists with other values")

