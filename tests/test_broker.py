import threading
from decimal import Decimal

import pytest

from app.broker import (
    Behavior,
    BrokerOrderNotFound,
    BrokerStatus,
    BrokerTimeout,
    MockBroker,
)
from app.db import connection

COID = "CO-sig-1"


@pytest.fixture
def broker_path(tmp_path) -> str:
    return str(tmp_path / "broker.db")


@pytest.fixture
def broker(broker_path) -> MockBroker:
    return MockBroker(broker_path)


def place(broker, coid=COID, qty="10", price="100.5", **kw):
    return broker.place_order(coid, "AAPL", "BUY", Decimal(qty), Decimal(price), **kw)


# --- behaviors ---------------------------------------------------------------------------

def test_accept_stores_accepted_order_with_seq_1(broker):
    order = place(broker, behavior=Behavior.ACCEPT)
    assert order.status is BrokerStatus.ACCEPTED
    assert order.filled_qty == Decimal("0")
    assert order.seq == 1
    assert order.broker_order_id.startswith("BRK-")
    assert broker.get_order(COID) == order


def test_reject_stores_rejected_order(broker):
    order = place(broker, behavior=Behavior.REJECT)
    assert order.status is BrokerStatus.REJECTED
    assert broker.get_order(COID).status is BrokerStatus.REJECTED


def test_partial_fill_fills_half(broker):
    order = place(broker, qty="10", behavior=Behavior.PARTIAL_FILL)
    assert order.status is BrokerStatus.PARTIALLY_FILLED
    assert order.filled_qty == Decimal("5")


def test_fill_fills_whole_qty(broker):
    order = place(broker, qty="7.5", behavior=Behavior.FILL)
    assert order.status is BrokerStatus.FILLED
    assert order.filled_qty == Decimal("7.5")


def test_accept_then_timeout_stores_order_and_raises(broker):
    with pytest.raises(BrokerTimeout):
        place(broker, behavior=Behavior.ACCEPT_THEN_TIMEOUT)
    # The response was lost, but the broker has the order. That's what get_order is for.
    assert broker.get_order(COID).status is BrokerStatus.ACCEPTED
    assert broker.orders_created == 1


def test_timeout_before_accept_raises_and_stores_nothing(broker):
    with pytest.raises(BrokerTimeout):
        place(broker, behavior=Behavior.TIMEOUT_BEFORE_ACCEPT)
    assert broker.get_order(COID) is None
    assert broker.orders_created == 0


def test_get_order_returns_none_for_unknown_id(broker):
    assert broker.get_order("CO-nope") is None


def test_decimals_are_stored_exactly(broker):
    order = place(broker, qty="0.1", price="12345678901234567890.123456789")
    stored = broker.get_order(COID)
    assert stored.qty * 3 == Decimal("0.3")
    assert stored.price == Decimal("12345678901234567890.123456789")
    assert stored == order


# --- configuration -----------------------------------------------------------------------

def test_default_behavior_applies_without_script(broker_path):
    broker = MockBroker(broker_path, default_behavior=Behavior.REJECT)
    assert place(broker).status is BrokerStatus.REJECTED


def test_scripted_behaviors_are_used_in_order_then_default(broker):
    broker.script(COID, Behavior.TIMEOUT_BEFORE_ACCEPT, Behavior.TIMEOUT_BEFORE_ACCEPT)
    for _ in range(2):
        with pytest.raises(BrokerTimeout):
            place(broker)
    assert place(broker).status is BrokerStatus.ACCEPTED  # script used up -> default ACCEPT


def test_script_only_affects_its_own_order(broker):
    broker.script(COID, Behavior.REJECT)
    assert place(broker, coid="CO-other").status is BrokerStatus.ACCEPTED
    assert place(broker, coid=COID).status is BrokerStatus.REJECTED


def test_per_call_behavior_overrides_script(broker):
    broker.script(COID, Behavior.REJECT)
    assert place(broker, behavior=Behavior.FILL).status is BrokerStatus.FILLED


# --- idempotency -------------------------------------------------------------------------

def test_same_client_order_id_returns_existing_order(broker):
    first = place(broker)
    second = place(broker)
    assert second == first
    assert broker.orders_created == 1
    assert broker.place_order_call_count == 2


def test_resend_with_different_behavior_does_not_change_existing_order(broker):
    place(broker, behavior=Behavior.ACCEPT)
    again = place(broker, behavior=Behavior.FILL)
    assert again.status is BrokerStatus.ACCEPTED
    assert broker.orders_created == 1


def test_resend_after_accept_then_timeout_returns_stored_order_without_raising(broker):
    with pytest.raises(BrokerTimeout):
        place(broker, behavior=Behavior.ACCEPT_THEN_TIMEOUT)
    assert place(broker, behavior=Behavior.ACCEPT_THEN_TIMEOUT).status is BrokerStatus.ACCEPTED
    assert broker.orders_created == 1


def test_resend_after_timeout_before_accept_creates_exactly_one_order(broker):
    broker.script(COID, Behavior.TIMEOUT_BEFORE_ACCEPT)
    with pytest.raises(BrokerTimeout):
        place(broker)
    place(broker)
    place(broker)
    assert broker.orders_created == 1
    assert broker.place_order_call_count == 3


def test_different_client_order_ids_create_separate_orders(broker):
    a = place(broker, coid="CO-a")
    b = place(broker, coid="CO-b")
    assert a.broker_order_id != b.broker_order_id
    assert broker.orders_created == 2


def test_concurrent_place_order_same_id_creates_one_order(broker):
    n = 20
    barrier = threading.Barrier(n)
    results, errors = [], []

    def worker():
        try:
            barrier.wait()  # release all threads together to make them race
            results.append(place(broker))
        except Exception as e:  # collected and asserted below; a thread can't fail the test
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    assert broker.orders_created == 1
    assert broker.place_order_call_count == n
    assert len({r.broker_order_id for r in results}) == 1


# --- persistence -------------------------------------------------------------------------

def test_orders_survive_new_broker_instance(broker_path):
    first = place(MockBroker(broker_path))
    restarted = MockBroker(broker_path)
    assert restarted.get_order(COID) == first
    assert restarted.orders_created == 1
    assert restarted.place_order_call_count == 0  # in-memory counter, by design


def test_broker_uses_its_own_file(tmp_path, broker_path):
    from app.db import init_schema

    gateway_path = str(tmp_path / "gateway.db")
    init_schema(gateway_path)
    place(MockBroker(broker_path))

    def tables(path):
        with connection(path) as c:
            return {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}

    assert "broker_orders" not in tables(gateway_path)
    assert tables(broker_path) == {"broker_orders"}


# --- make_update -------------------------------------------------------------------------

def test_make_update_increments_seq_and_changes_order(broker):
    order = place(broker, qty="10")
    u1 = broker.make_update(COID, BrokerStatus.PARTIALLY_FILLED, Decimal("4"))
    u2 = broker.make_update(COID, BrokerStatus.FILLED, Decimal("10"))

    assert (u1["seq"], u2["seq"]) == (2, 3)
    assert u1 == {
        "client_order_id": COID,
        "broker_order_id": order.broker_order_id,
        "seq": 2,
        "status": "PARTIALLY_FILLED",
        "cumulative_filled_qty": "4",
    }
    stored = broker.get_order(COID)
    assert (stored.status, stored.filled_qty, stored.seq) == (BrokerStatus.FILLED, Decimal("10"), 3)


def test_make_update_for_unknown_order_raises(broker):
    with pytest.raises(BrokerOrderNotFound):
        broker.make_update("CO-nope", BrokerStatus.FILLED, Decimal("1"))


@pytest.mark.parametrize("filled", ["-1", "10.0001"])
def test_make_update_with_fill_outside_qty_raises_and_changes_nothing(broker, filled):
    place(broker, qty="10")
    with pytest.raises(ValueError):
        broker.make_update(COID, BrokerStatus.FILLED, Decimal(filled))
    stored = broker.get_order(COID)
    assert (stored.status, stored.seq) == (BrokerStatus.ACCEPTED, 1)


def test_concurrent_make_update_never_reuses_a_seq(broker):
    place(broker, qty="100")
    n = 10
    barrier = threading.Barrier(n)
    seqs, errors = [], []

    def worker(i):
        try:
            barrier.wait()
            seqs.append(broker.make_update(COID, BrokerStatus.PARTIALLY_FILLED, Decimal(i))["seq"])
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    assert sorted(seqs) == list(range(2, 2 + n))
    assert broker.get_order(COID).seq == 1 + n
