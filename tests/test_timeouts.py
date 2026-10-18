import threading
from decimal import Decimal

import pytest

from app.broker import Behavior, BrokerStatus, BrokerTimeout
from app.models import OrderStatus
from tests.helpers import event_names, kill_switch, signal

S = OrderStatus
COID = "CO-sig-1"


@pytest.fixture
def calls(broker, monkeypatch):
    """Records every broker call in order as (method, client_order_id)."""
    log = []
    real_place, real_get = broker.place_order, broker.get_order

    def place_order(client_order_id, *args, **kwargs):
        log.append(("place_order", client_order_id))
        return real_place(client_order_id, *args, **kwargs)

    def get_order(client_order_id):
        log.append(("get_order", client_order_id))
        return real_get(client_order_id)

    monkeypatch.setattr(broker, "place_order", place_order)
    monkeypatch.setattr(broker, "get_order", get_order)
    return log


# --- the lost-response case --------------------------------------------------------------

def test_accept_then_timeout_resolves_from_broker_without_second_place_order(
        service, broker, calls):
    broker.script(COID, Behavior.ACCEPT_THEN_TIMEOUT)

    order = service.submit(signal())
    gateway_calls = list(calls)  # snapshot before this test does its own get_order below

    at_broker = broker.get_order(COID)
    assert broker.orders_created == 1
    assert broker.place_order_call_count == 1  # never sent a second time
    assert order.status is S.ACCEPTED
    assert order.status.value == at_broker.status.value  # our final status matches the broker
    assert order.filled_qty == at_broker.filled_qty
    assert order.broker_order_id == at_broker.broker_order_id
    assert order.last_seq == at_broker.seq
    assert gateway_calls == [("place_order", COID), ("get_order", COID)]
    assert event_names(service) == ["RECEIVED", "CLAIMED", "BROKER_TIMEOUT",
                                    "RESOLVED_FROM_BROKER"]


def test_timeout_sets_unknown_before_asking_broker(service, broker, monkeypatch):
    broker.script(COID, Behavior.ACCEPT_THEN_TIMEOUT)
    seen = {}
    real_get = broker.get_order

    def get_order(client_order_id):
        seen["status"] = service.get("sig-1").status  # what's committed at lookup time
        return real_get(client_order_id)

    monkeypatch.setattr(broker, "get_order", get_order)
    service.submit(signal())
    assert seen["status"] is S.UNKNOWN


def test_lost_response_adopts_broker_fill_that_happened_meanwhile(service, broker, monkeypatch):
    # The broker accepted AND filled the order before we managed to ask about it.
    real_place = broker.place_order

    def place_then_fill(client_order_id, instrument, side, qty, price, **kw):
        try:
            return real_place(client_order_id, instrument, side, qty, price,
                              behavior=Behavior.ACCEPT_THEN_TIMEOUT)
        finally:
            broker.make_update(client_order_id, BrokerStatus.FILLED, qty)

    monkeypatch.setattr(broker, "place_order", place_then_fill)
    order = service.submit(signal(qty="2"))

    assert order.status is S.FILLED
    assert order.filled_qty == Decimal("2")
    assert order.last_seq == 2
    assert broker.orders_created == 1


# --- the request-never-arrived case ------------------------------------------------------

def test_timeout_before_accept_retries_with_same_client_order_id(service, broker, calls):
    broker.script(COID, Behavior.TIMEOUT_BEFORE_ACCEPT)

    order = service.submit(signal())

    assert order.status is S.ACCEPTED
    assert order.submit_attempts == 2
    assert broker.orders_created == 1
    # get_order comes between the two sends, and both sends use the same ID.
    assert calls == [("place_order", COID), ("get_order", COID), ("place_order", COID)]
    assert event_names(service) == ["RECEIVED", "CLAIMED", "BROKER_TIMEOUT", "RETRY_CLAIMED",
                                    "BROKER_RESPONSE"]


def test_retry_that_times_out_again_stays_unknown_without_looping(service, broker, calls):
    broker.script(COID, Behavior.TIMEOUT_BEFORE_ACCEPT, Behavior.TIMEOUT_BEFORE_ACCEPT)

    order = service.submit(signal())

    assert order.status is S.UNKNOWN
    assert broker.place_order_call_count == 2  # one send + one retry, then stop
    assert broker.orders_created == 0

    # A later resolve (e.g. from reconciliation) asks again and retries once more.
    assert service.resolve_unknown(order).status is S.ACCEPTED
    assert broker.orders_created == 1
    assert calls[-2:] == [("get_order", COID), ("place_order", COID)]


def test_duplicate_submit_of_unknown_order_does_not_resend(service, broker):
    broker.script(COID, Behavior.TIMEOUT_BEFORE_ACCEPT, Behavior.TIMEOUT_BEFORE_ACCEPT)
    service.submit(signal())
    assert service.submit(signal()).status is S.UNKNOWN
    assert broker.place_order_call_count == 2


# --- kill switch -------------------------------------------------------------------------

def test_retry_is_skipped_while_kill_switch_is_on(service, broker, db_path, monkeypatch):
    real_place = broker.place_order

    def switch_on_then_timeout(*args, **kwargs):
        kill_switch(db_path, True)  # operator hits the switch while our request is in flight
        return real_place(*args, behavior=Behavior.TIMEOUT_BEFORE_ACCEPT)

    monkeypatch.setattr(broker, "place_order", switch_on_then_timeout)
    order = service.submit(signal())

    assert order.status is S.UNKNOWN
    assert broker.place_order_call_count == 1
    assert event_names(service)[-1] == "RETRY_SKIPPED_KILL_SWITCH"


def test_lookup_still_runs_while_kill_switch_is_on(service, broker, db_path, monkeypatch):
    # The kill switch stops new sends, not monitoring: a lost-response order is still resolved.
    real_place = broker.place_order

    def switch_on_then_accept_and_timeout(*args, **kwargs):
        kill_switch(db_path, True)
        return real_place(*args, behavior=Behavior.ACCEPT_THEN_TIMEOUT)

    monkeypatch.setattr(broker, "place_order", switch_on_then_accept_and_timeout)
    assert service.submit(signal()).status is S.ACCEPTED


# --- robustness --------------------------------------------------------------------------

def test_failed_lookup_leaves_order_unknown_and_does_not_resend(service, broker, monkeypatch):
    broker.script(COID, Behavior.TIMEOUT_BEFORE_ACCEPT)

    def get_order_times_out(client_order_id):
        raise BrokerTimeout(client_order_id)

    monkeypatch.setattr(broker, "get_order", get_order_times_out)
    order = service.submit(signal())

    assert order.status is S.UNKNOWN
    assert broker.place_order_call_count == 1  # without an answer we never resend
    assert event_names(service)[-1] == "BROKER_LOOKUP_FAILED"


@pytest.mark.parametrize("behavior, status", [(Behavior.ACCEPT, S.ACCEPTED),
                                              (Behavior.REJECT, S.REJECTED)])
def test_resolve_unknown_ignores_orders_that_are_not_unknown(service, broker, calls,
                                                             behavior, status):
    broker.script(COID, behavior)
    order = service.submit(signal())
    calls.clear()
    assert service.resolve_unknown(order).status is status
    assert calls == []  # didn't even ask the broker


def test_concurrent_resolve_unknown_resends_only_once(service, broker, monkeypatch):
    broker.script(COID, Behavior.TIMEOUT_BEFORE_ACCEPT, Behavior.TIMEOUT_BEFORE_ACCEPT)
    stuck = service.submit(signal())
    assert stuck.status is S.UNKNOWN
    sends_before = broker.place_order_call_count

    # Force the worst case: all N resolvers have heard "not found" before any of them claims.
    n = 10
    after_lookup = threading.Barrier(n, timeout=10)
    real_get = broker.get_order

    def gated_get(client_order_id):
        result = real_get(client_order_id)
        after_lookup.wait()
        return result

    monkeypatch.setattr(broker, "get_order", gated_get)
    results, errors = [], []

    def worker():
        try:
            results.append(service.resolve_unknown(stuck))
        except Exception as e:  # collected and asserted below
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    assert broker.place_order_call_count - sends_before == 1
    assert broker.orders_created == 1
    assert service.get("sig-1").status is S.ACCEPTED
