import json
import threading
from decimal import Decimal

import pytest
from pydantic import ValidationError

from app.broker import Behavior, BrokerStatus, BrokerTimeout
from app.models import OrderStatus
from app.service import IgnoreReason
from tests.helpers import signal

S = OrderStatus
R = IgnoreReason
COID = "CO-sig-1"


@pytest.fixture
def accepted(service, broker):
    """An ACCEPTED order for qty 10 (last_seq 1, from the place_order response)."""
    order = service.submit(signal(qty="10", price="10"))
    assert (order.status, order.last_seq) == (S.ACCEPTED, 1)
    return order


def upd(seq, status, filled, coid=COID):
    return {"client_order_id": coid, "seq": seq, "status": status,
            "cumulative_filled_qty": filled}


def update_events(service):
    return [(e["event"], json.loads(e["detail"]).get("reason"))
            for e in service.events("sig-1") if e["event"].startswith("BROKER_UPDATE")]


def stored(service):
    o = service.get("sig-1")
    return o.status, o.filled_qty, o.last_seq


# --- applying ------------------------------------------------------------------------------

def test_partial_fill_update_is_applied(service, broker, accepted):
    result = service.handle_update(broker.make_update(COID, BrokerStatus.PARTIALLY_FILLED,
                                                      Decimal("4")))
    assert result.applied and result.reason is None
    assert stored(service) == (S.PARTIALLY_FILLED, Decimal("4"), 2)


def test_successive_fills_store_cumulative_value_not_sum(service, broker, accepted):
    service.handle_update(broker.make_update(COID, BrokerStatus.PARTIALLY_FILLED, Decimal("4")))
    service.handle_update(broker.make_update(COID, BrokerStatus.PARTIALLY_FILLED, Decimal("7")))
    assert stored(service) == (S.PARTIALLY_FILLED, Decimal("7"), 3)  # 7, not 4 + 7


def test_seq_gap_is_applied_because_fills_are_cumulative(service, accepted):
    # seq 2 (fill 4) was lost; seq 3 already contains it.
    assert service.handle_update(upd(3, "PARTIALLY_FILLED", "6")).applied
    assert stored(service) == (S.PARTIALLY_FILLED, Decimal("6"), 3)


# --- duplicates and ordering ---------------------------------------------------------------

def test_replayed_partial_fill_leaves_filled_qty_unchanged(service, broker, accepted):
    u = broker.make_update(COID, BrokerStatus.PARTIALLY_FILLED, Decimal("4"))
    assert service.handle_update(u).applied

    replay = service.handle_update(u)

    assert not replay.applied and replay.reason is R.STALE_OR_DUPLICATE
    assert stored(service) == (S.PARTIALLY_FILLED, Decimal("4"), 2)
    assert update_events(service) == [("BROKER_UPDATE_APPLIED", None),
                                      ("BROKER_UPDATE_IGNORED", "STALE_OR_DUPLICATE")]


def test_seq_2_before_seq_1_applies_2_and_ignores_1(service, broker, monkeypatch):
    # An order whose place_order response was lost and whose lookup failed: UNKNOWN, last_seq 0.
    # The broker has it at seq 1 (ACCEPTED).
    broker.script(COID, Behavior.ACCEPT_THEN_TIMEOUT)
    real_get = broker.get_order

    def lookup_times_out(client_order_id):
        raise BrokerTimeout(client_order_id)

    monkeypatch.setattr(broker, "get_order", lookup_times_out)
    assert service.submit(signal(qty="10", price="10")).status is S.UNKNOWN
    monkeypatch.setattr(broker, "get_order", real_get)

    at_broker = broker.get_order(COID)
    seq1 = upd(1, "ACCEPTED", "0")
    seq2 = broker.make_update(COID, BrokerStatus.PARTIALLY_FILLED, Decimal("4"))
    assert (at_broker.seq, seq2["seq"]) == (1, 2)

    first = service.handle_update(seq2)  # arrives first
    late = service.handle_update(seq1)

    assert first.applied
    assert not late.applied and late.reason is R.STALE_OR_DUPLICATE
    o = service.get("sig-1")
    assert (o.status, o.filled_qty, o.last_seq) == (S.PARTIALLY_FILLED, Decimal("4"), 2)
    assert o.broker_order_id == at_broker.broker_order_id  # learned from the update


def test_out_of_order_updates_end_in_latest_state(service, broker, accepted):
    u2 = broker.make_update(COID, BrokerStatus.PARTIALLY_FILLED, Decimal("4"))
    u3 = broker.make_update(COID, BrokerStatus.FILLED, Decimal("10"))
    service.handle_update(u3)
    service.handle_update(u2)
    assert stored(service) == (S.FILLED, Decimal("10"), 3)


def test_older_update_after_filled_leaves_order_filled(service, broker, accepted):
    u2 = broker.make_update(COID, BrokerStatus.PARTIALLY_FILLED, Decimal("4"))
    u3 = broker.make_update(COID, BrokerStatus.FILLED, Decimal("10"))
    service.handle_update(u2)
    service.handle_update(u3)

    late = service.handle_update(u2)

    assert not late.applied
    assert stored(service) == (S.FILLED, Decimal("10"), 3)


def test_newer_update_after_terminal_is_ignored(service, accepted):
    service.handle_update(upd(2, "REJECTED", "0"))
    result = service.handle_update(upd(3, "PARTIALLY_FILLED", "4"))  # higher seq, still ignored
    assert result.reason is R.ORDER_TERMINAL
    assert stored(service) == (S.REJECTED, Decimal("0"), 2)


# --- validation ----------------------------------------------------------------------------

def test_decreasing_fill_is_ignored(service, accepted):
    service.handle_update(upd(2, "PARTIALLY_FILLED", "6"))
    result = service.handle_update(upd(3, "PARTIALLY_FILLED", "5"))
    assert result.reason is R.FILL_DECREASED
    assert stored(service) == (S.PARTIALLY_FILLED, Decimal("6"), 2)  # last_seq not advanced


def test_fill_exceeding_qty_is_ignored(service, accepted):
    result = service.handle_update(upd(2, "FILLED", "10.0001"))
    assert result.reason is R.FILL_EXCEEDS_QTY
    assert stored(service) == (S.ACCEPTED, Decimal("0"), 1)


def test_backwards_transition_is_ignored(service, accepted):
    service.handle_update(upd(2, "PARTIALLY_FILLED", "4"))
    result = service.handle_update(upd(3, "ACCEPTED", "4"))
    assert result.reason is R.INVALID_TRANSITION
    assert stored(service)[0] is S.PARTIALLY_FILLED


@pytest.mark.parametrize("status, filled", [
    ("FILLED", "9"),             # FILLED must mean all of qty
    ("PARTIALLY_FILLED", "10"),  # a full fill must say FILLED
    ("PARTIALLY_FILLED", "0"),
    ("REJECTED", "3"),
])
def test_fill_inconsistent_with_status_is_ignored(service, accepted, status, filled):
    result = service.handle_update(upd(2, status, filled))
    assert result.reason is R.FILL_INCONSISTENT_WITH_STATUS
    assert stored(service) == (S.ACCEPTED, Decimal("0"), 1)


@pytest.mark.parametrize("bad", [
    {"seq": 0}, {"status": "CANCELLED"}, {"status": "SUBMITTING"},
    {"cumulative_filled_qty": "-1"}, {"cumulative_filled_qty": "NaN"},
])
def test_malformed_update_raises_validation_error(service, accepted, bad):
    with pytest.raises(ValidationError):
        service.handle_update({**upd(2, "PARTIALLY_FILLED", "4"), **bad})
    assert stored(service) == (S.ACCEPTED, Decimal("0"), 1)


def test_update_for_unknown_order_is_handled_safely(service, accepted):
    result = service.handle_update(upd(5, "FILLED", "10", coid="CO-not-ours"))

    assert not result.applied
    assert result.reason is R.UNKNOWN_ORDER
    assert result.order is None
    assert stored(service) == (S.ACCEPTED, Decimal("0"), 1)  # other orders untouched
    assert service.get("not-ours") is None  # no row invented for it
    assert update_events(service) == []


# --- unresolved orders & logging -----------------------------------------------------------

def test_update_resolves_an_unknown_order(service, broker):
    broker.script(COID, Behavior.TIMEOUT_BEFORE_ACCEPT, Behavior.TIMEOUT_BEFORE_ACCEPT)
    assert service.submit(signal(qty="10", price="10")).status is S.UNKNOWN
    result = service.handle_update({**upd(1, "FILLED", "10"), "broker_order_id": "BRK-1"})
    assert result.applied
    assert result.order.status is S.FILLED and result.order.broker_order_id == "BRK-1"


def test_every_update_is_logged_with_reason(service, accepted):
    service.handle_update(upd(2, "PARTIALLY_FILLED", "4"))   # applied
    service.handle_update(upd(2, "PARTIALLY_FILLED", "4"))   # replay
    service.handle_update(upd(3, "PARTIALLY_FILLED", "3"))   # fill went down
    service.handle_update(upd(4, "FILLED", "11"))            # more than qty
    service.handle_update(upd(5, "FILLED", "10"))            # applied
    service.handle_update(upd(6, "FILLED", "10"))            # after terminal
    assert update_events(service) == [
        ("BROKER_UPDATE_APPLIED", None),
        ("BROKER_UPDATE_IGNORED", "STALE_OR_DUPLICATE"),
        ("BROKER_UPDATE_IGNORED", "FILL_DECREASED"),
        ("BROKER_UPDATE_IGNORED", "FILL_EXCEEDS_QTY"),
        ("BROKER_UPDATE_APPLIED", None),
        ("BROKER_UPDATE_IGNORED", "ORDER_TERMINAL"),
    ]
    applied = [e for e in service.events("sig-1") if e["event"] == "BROKER_UPDATE_APPLIED"]
    assert [(e["from_status"], e["to_status"]) for e in applied] == [
        ("ACCEPTED", "PARTIALLY_FILLED"), ("PARTIALLY_FILLED", "FILLED")]


# --- concurrency ---------------------------------------------------------------------------

def test_concurrent_delivery_of_same_update_applies_once(service, broker, accepted):
    u = broker.make_update(COID, BrokerStatus.PARTIALLY_FILLED, Decimal("4"))
    n = 10
    barrier = threading.Barrier(n)
    results, errors = [], []

    def worker():
        try:
            barrier.wait()
            results.append(service.handle_update(u))
        except Exception as e:  # collected and asserted below
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    assert sum(r.applied for r in results) == 1
    assert {r.reason for r in results if not r.applied} == {R.STALE_OR_DUPLICATE}
    assert stored(service) == (S.PARTIALLY_FILLED, Decimal("4"), 2)
