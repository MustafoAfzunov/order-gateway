import threading
from decimal import Decimal

import pytest

from app.broker import Behavior, BrokerStatus, BrokerTimeout, MockBroker
from app.db import connection, init_schema
from app.models import OrderStatus
from app.service import OrderService
from tests.helpers import event_names, kill_switch, signal

S = OrderStatus
COID = "CO-sig-1"


class SimulatedCrash(BaseException):
    """Stands in for the process dying (kill -9, power loss). It's a BaseException so no
    `except Exception` in the code under test can swallow it, just as nothing survives a
    real crash."""


@pytest.fixture
def restart(service, broker):
    """Build a brand-new OrderService and MockBroker on the SAME DB files, as after a process
    restart. Nothing in memory survives: broker scripts, call counters, the in-flight set."""
    def _restart():
        init_schema(service.db_path)  # what app start-up does
        new_broker = MockBroker(broker.db_path)
        return OrderService(service.db_path, new_broker, service.settings), new_broker
    return _restart


def lookup_times_out(client_order_id):
    raise BrokerTimeout(client_order_id)


# --- recovery after restart ----------------------------------------------------------------

def test_timeout_then_restart_reconcile_recovers_without_duplicate(
        service, broker, restart, monkeypatch):
    # Response lost AND the follow-up lookup failed, so the order is stuck in UNKNOWN.
    broker.script(COID, Behavior.ACCEPT_THEN_TIMEOUT)
    monkeypatch.setattr(broker, "get_order", lookup_times_out)
    assert service.submit(signal()).status is S.UNKNOWN

    service2, broker2 = restart()
    results = service2.reconcile()

    order, at_broker = service2.get("sig-1"), broker2.get_order(COID)
    assert order.status is S.ACCEPTED
    assert (order.status.value, order.filled_qty, order.last_seq, order.broker_order_id) == (
        at_broker.status.value, at_broker.filled_qty, at_broker.seq, at_broker.broker_order_id)
    assert broker2.orders_created == 1        # no duplicate
    assert broker2.place_order_call_count == 0  # recovered by asking, not by resending
    assert [(r.status_before, r.status_after) for r in results] == [(S.UNKNOWN, S.ACCEPTED)]


def test_crash_during_broker_call_recovers_order_the_broker_has(
        service, broker, restart, monkeypatch):
    real_place = broker.place_order

    def broker_stores_then_we_crash(*args, **kwargs):
        real_place(*args, **kwargs)
        raise SimulatedCrash

    monkeypatch.setattr(broker, "place_order", broker_stores_then_we_crash)
    with pytest.raises(SimulatedCrash):
        service.submit(signal())
    assert service.get("sig-1").status is S.SUBMITTING  # write-ahead state left by the crash

    service2, broker2 = restart()
    service2.reconcile()

    assert service2.get("sig-1").status is S.ACCEPTED
    assert broker2.orders_created == 1
    assert broker2.place_order_call_count == 0
    assert "ORPHANED_SUBMITTING" in event_names(service2)


def test_crash_before_request_reached_broker_resends_once_with_same_id(
        service, broker, restart, monkeypatch):
    def crash_before_sending(*args, **kwargs):
        raise SimulatedCrash

    monkeypatch.setattr(broker, "place_order", crash_before_sending)
    with pytest.raises(SimulatedCrash):
        service.submit(signal())

    service2, broker2 = restart()
    service2.reconcile()
    service2.reconcile()  # a second run must not send again

    assert service2.get("sig-1").status is S.ACCEPTED
    assert broker2.place_order_call_count == 1
    assert broker2.orders_created == 1
    assert broker2.get_order(COID) is not None  # sent under the same client_order_id


def test_reconcile_after_restart_adopts_fills_missed_while_down(service, broker, restart):
    service.submit(signal(qty="10", price="10"))
    broker.make_update(COID, BrokerStatus.PARTIALLY_FILLED, Decimal("3"))  # push never arrived
    broker.make_update(COID, BrokerStatus.PARTIALLY_FILLED, Decimal("7"))

    service2, _ = restart()
    service2.reconcile()

    o = service2.get("sig-1")
    assert (o.status, o.filled_qty, o.last_seq) == (S.PARTIALLY_FILLED, Decimal("7"), 3)


def test_concurrent_reconciles_after_restart_resend_once(service, broker, restart, monkeypatch):
    def crash_before_sending(*args, **kwargs):
        raise SimulatedCrash

    monkeypatch.setattr(broker, "place_order", crash_before_sending)
    with pytest.raises(SimulatedCrash):
        service.submit(signal())

    service2, broker2 = restart()
    n = 5
    barrier = threading.Barrier(n)
    errors = []

    def worker():
        try:
            barrier.wait()
            service2.reconcile()
        except Exception as e:  # collected and asserted below
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    assert broker2.place_order_call_count == 1
    assert service2.get("sig-1").status is S.ACCEPTED


def test_stale_orphan_snapshot_cannot_hijack_a_newer_attempt(
        service, broker, restart, monkeypatch):
    """ABA, forced deterministically. The test above hits it only sometimes.

    B sees the orphan (SUBMITTING, attempt 1) and passes its in-flight check, then pauses.
    A recovers the same orphan and re-claims it (SUBMITTING, attempt 2), and is mid-send.
    B resumes. Its compare-and-set on the OLD SUBMITTING must fail, or B would mark A's live
    attempt UNKNOWN, get "not found" (A's request hasn't landed) and send a second time.
    """
    def crash_before_sending(*args, **kwargs):
        raise SimulatedCrash

    monkeypatch.setattr(broker, "place_order", crash_before_sending)
    with pytest.raises(SimulatedCrash):
        service.submit(signal())

    service2, broker2 = restart()
    b_checked, a_sending, b_done = threading.Event(), threading.Event(), threading.Event()
    real_in_flight, real_place = service2._is_in_flight, broker2.place_order

    def in_flight(coid):
        result = real_in_flight(coid)
        if threading.current_thread().name == "B":
            b_checked.set()
            assert a_sending.wait(5), "A never reached place_order"
        return result

    def place_order(*args, **kwargs):
        if threading.current_thread().name == "A":
            a_sending.set()
            assert b_done.wait(5), "B never finished"
        return real_place(*args, **kwargs)

    monkeypatch.setattr(service2, "_is_in_flight", in_flight)
    monkeypatch.setattr(broker2, "place_order", place_order)
    notes, errors = {}, []

    def run(name, after=None):
        try:
            notes[name] = service2.reconcile()[0].note
        except BaseException as e:  # collected and asserted below
            errors.append(e)
        finally:
            if after:
                after.set()

    b = threading.Thread(target=run, args=("B", b_done), name="B")
    a = threading.Thread(target=run, args=("A",), name="A")
    b.start()
    assert b_checked.wait(5)
    a.start()
    a.join(10)
    b.join(10)

    assert errors == []
    assert broker2.place_order_call_count == 1
    assert notes["B"] == "changed concurrently -> SUBMITTING"
    assert service2.get("sig-1").status is S.ACCEPTED


# --- kill switch ---------------------------------------------------------------------------

def test_kill_switch_on_reconcile_still_updates_existing_orders(
        service, broker, db_path, monkeypatch):
    # Order A: live at the broker, then filled, but the push was missed.
    service.submit(signal("sig-a", qty="10", price="10"))
    broker.make_update("CO-sig-a", BrokerStatus.FILLED, Decimal("10"))
    # Order B: response lost and lookup failed, so it's UNKNOWN although the broker has it.
    broker.script("CO-sig-b", Behavior.ACCEPT_THEN_TIMEOUT)
    monkeypatch.setattr(broker, "get_order", lookup_times_out)
    assert service.submit(signal("sig-b")).status is S.UNKNOWN
    monkeypatch.undo()
    sends_before = broker.place_order_call_count

    kill_switch(db_path, True)
    service.reconcile()

    assert service.get("sig-a").status is S.FILLED
    assert service.get("sig-a").filled_qty == Decimal("10")
    assert service.get("sig-b").status is S.ACCEPTED
    assert broker.place_order_call_count == sends_before  # nothing sent


def test_kill_switch_on_blocks_only_the_resend(service, broker, restart, db_path, monkeypatch):
    def crash_before_sending(*args, **kwargs):
        raise SimulatedCrash

    monkeypatch.setattr(broker, "place_order", crash_before_sending)
    with pytest.raises(SimulatedCrash):
        service.submit(signal())

    service2, broker2 = restart()
    kill_switch(db_path, True)
    service2.reconcile()
    assert service2.get("sig-1").status is S.UNKNOWN
    assert broker2.place_order_call_count == 0
    assert event_names(service2)[-1] == "RETRY_SKIPPED_KILL_SWITCH"

    kill_switch(db_path, False)
    service2.reconcile()
    assert service2.get("sig-1").status is S.ACCEPTED
    assert broker2.place_order_call_count == 1


# --- what reconcile leaves alone -----------------------------------------------------------

def test_reconcile_skips_order_being_sent_by_this_process(service, broker, monkeypatch):
    real_place = broker.place_order
    seen = {}

    def reconcile_while_sending(*args, **kwargs):
        seen["results"] = service.reconcile()  # e.g. the periodic job fires mid-request
        return real_place(*args, **kwargs)

    monkeypatch.setattr(broker, "place_order", reconcile_while_sending)
    assert service.submit(signal()).status is S.ACCEPTED

    assert [(r.status_before, r.note) for r in seen["results"]] == [
        (S.SUBMITTING, "skipped: in flight")]
    assert broker.place_order_call_count == 1
    assert "ORPHANED_SUBMITTING" not in event_names(service)


def test_reconcile_does_not_query_terminal_orders(service, broker, db_path, monkeypatch):
    broker.script("CO-f", Behavior.FILL)
    broker.script("CO-r", Behavior.REJECT)
    service.submit(signal("f"))
    service.submit(signal("r"))
    service.submit(signal("risk", qty="11"))  # REJECTED_RISK
    kill_switch(db_path, True)
    service.submit(signal("blocked"))          # BLOCKED

    asked = []
    monkeypatch.setattr(broker, "get_order", lambda coid: asked.append(coid))
    assert service.reconcile() == []
    assert asked == []


def test_new_order_not_at_broker_is_left_new_and_not_sent(service, broker, db_path):
    with connection(db_path) as c:  # crash between INSERT and claim
        c.execute("INSERT INTO orders (signal_id, client_order_id, instrument, side, qty,"
                  " reference_price, status, created_at, updated_at)"
                  " VALUES ('sig-1', 'CO-sig-1', 'AAPL', 'BUY', '2', '100', 'NEW', 't', 't')")
    [result] = service.reconcile()
    assert (result.status_after, result.note) == (S.NEW, "not at broker (never sent)")
    assert broker.place_order_call_count == 0


def test_in_sync_order_produces_no_events(service, broker):
    service.submit(signal())
    events_before = len(service.events("sig-1"))
    [result] = service.reconcile()
    assert result.note == "in sync"
    assert len(service.events("sig-1")) == events_before


def test_order_missing_at_broker_is_flagged_and_left_unchanged(service, broker):
    service.submit(signal())
    with connection(broker.db_path) as c:
        c.execute("DELETE FROM broker_orders")  # the broker "lost" a confirmed order
    [result] = service.reconcile()
    assert result.note == "missing at broker"
    assert service.get("sig-1").status is S.ACCEPTED
    assert event_names(service)[-1] == "RECONCILE_MISSING_AT_BROKER"
    assert broker.place_order_call_count == 1  # no resend of a confirmed order


def test_one_failing_order_does_not_stop_reconcile(service, broker, monkeypatch):
    service.submit(signal("a", qty="10", price="10"))
    service.submit(signal("b", qty="10", price="10"))
    broker.make_update("CO-b", BrokerStatus.FILLED, Decimal("10"))
    real_get = broker.get_order

    def get_order(coid):
        if coid == "CO-a":
            raise RuntimeError("broker API error")
        return real_get(coid)

    monkeypatch.setattr(broker, "get_order", get_order)
    results = {r.signal_id: r for r in service.reconcile()}

    assert results["a"].note.startswith("error:")
    assert results["b"].status_after is S.FILLED


def test_unexpected_broker_error_leaves_submitting_and_reconcile_recovers(
        service, broker, monkeypatch):
    # Not a timeout, but just as uncertain: did the request reach the broker before it failed?
    real_place = broker.place_order

    def stores_then_connection_drops(*args, **kwargs):
        real_place(*args, **kwargs)
        raise ConnectionError("connection reset by peer")

    monkeypatch.setattr(broker, "place_order", stores_then_connection_drops)
    with pytest.raises(ConnectionError):
        service.submit(signal())
    assert service.get("sig-1").status is S.SUBMITTING  # nothing guessed
    monkeypatch.undo()

    # Same process, no restart: it's no longer in flight, so reconcile treats it as orphaned.
    service.reconcile()
    assert service.get("sig-1").status is S.ACCEPTED
    assert broker.orders_created == 1
    assert broker.place_order_call_count == 1  # recovered by lookup, not by resending


def test_broker_snapshot_older_than_ours_is_reported_and_ignored(service):
    service.submit(signal(qty="10", price="10"))  # broker is at seq 1
    service.handle_update({"client_order_id": COID, "seq": 2, "status": "PARTIALLY_FILLED",
                           "cumulative_filled_qty": "4"})  # we're at seq 2
    [result] = service.reconcile()
    assert result.note == "broker snapshot ignored: STALE_OR_DUPLICATE"
    o = service.get("sig-1")
    assert (o.status, o.filled_qty, o.last_seq) == (S.PARTIALLY_FILLED, Decimal("4"), 2)
