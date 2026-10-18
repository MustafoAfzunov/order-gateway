import threading
from decimal import Decimal

import pytest

from app.broker import Behavior
from app.db import connection
from app.models import OrderStatus
from app.service import SignalConflict
from tests.helpers import event_names, kill_switch, signal

S = OrderStatus


# --- happy path & broker responses -------------------------------------------------------

def test_submit_new_signal_places_one_broker_order(service, broker):
    order = service.submit(signal())
    assert order.status is S.ACCEPTED
    assert order.client_order_id == "CO-sig-1"
    assert order.broker_order_id == broker.get_order("CO-sig-1").broker_order_id
    assert order.last_seq == 1
    assert order.submit_attempts == 1
    assert broker.orders_created == 1


@pytest.mark.parametrize("behavior, status, filled", [
    (Behavior.REJECT, S.REJECTED, "0"),
    (Behavior.PARTIAL_FILL, S.PARTIALLY_FILLED, "1"),
    (Behavior.FILL, S.FILLED, "2"),
])
def test_broker_response_is_mapped_to_order_status(service, broker, behavior, status, filled):
    broker.script("CO-sig-1", behavior)
    order = service.submit(signal(qty="2"))
    assert order.status is status
    assert order.filled_qty == Decimal(filled)


def test_submitting_is_committed_before_broker_call(service, broker, db_path, monkeypatch):
    seen = {}
    real_place_order = broker.place_order

    def spy(client_order_id, *args, **kwargs):
        # A separate connection only sees committed data, so this proves write-ahead.
        with connection(db_path) as c:
            seen["status"] = c.execute(
                "SELECT status FROM orders WHERE client_order_id = ?", (client_order_id,)
            ).fetchone()[0]
        return real_place_order(client_order_id, *args, **kwargs)

    monkeypatch.setattr(broker, "place_order", spy)
    service.submit(signal())
    assert seen["status"] == "SUBMITTING"


def test_every_step_is_logged_as_an_order_event(service):
    service.submit(signal())
    events = service.events("sig-1")
    assert [(e["event"], e["from_status"], e["to_status"]) for e in events] == [
        ("RECEIVED", None, "NEW"),
        ("CLAIMED", "NEW", "SUBMITTING"),
        ("BROKER_RESPONSE", "SUBMITTING", "ACCEPTED"),
    ]


# --- duplicates & concurrency ------------------------------------------------------------

def test_duplicate_submission_produces_one_broker_order(service, broker):
    first = service.submit(signal())
    second = service.submit(signal())
    assert second.id == first.id
    assert second.status is S.ACCEPTED
    assert broker.place_order_call_count == 1  # the duplicate never reached the broker
    assert broker.orders_created == 1
    assert event_names(service).count("CLAIMED") == 1


def test_duplicate_with_equal_decimal_spelling_is_a_duplicate(service, broker):
    service.submit(signal(qty="2", price="100"))
    service.submit(signal(qty="2.00", price="100.0"))
    assert broker.place_order_call_count == 1


@pytest.mark.parametrize("changed", [{"qty": "3"}, {"price": "101"}, {"side": "SELL"},
                                     {"symbol": "MSFT"}])
def test_same_signal_id_with_different_payload_raises_conflict(service, broker, changed):
    service.submit(signal())
    with pytest.raises(SignalConflict):
        service.submit(signal(**changed))
    assert broker.place_order_call_count == 1


def test_concurrent_submissions_same_signal_produce_one_broker_order(service, broker):
    n = 10
    barrier = threading.Barrier(n)
    results, errors = [], []

    def worker():
        try:
            barrier.wait()  # all threads start submit() together
            results.append(service.submit(signal()))
        except Exception as e:  # collected and asserted below
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    assert len(results) == n
    assert {r.id for r in results} == {results[0].id}
    assert broker.place_order_call_count == 1  # only the claim winner called the broker
    assert broker.orders_created == 1
    assert event_names(service).count("RECEIVED") == 1
    assert event_names(service).count("CLAIMED") == 1
    assert service.get("sig-1").status is S.ACCEPTED


def test_atomic_claim_lets_only_one_of_n_racing_callers_reach_broker(service, broker, monkeypatch):
    # The test above races naturally, but SQLite serialises writers, so most threads see the
    # winner's SUBMITTING and return early. Remove the claim's WHERE status='NEW' and that test
    # still passes. This one forces the worst case: every thread has read status=NEW before
    # any of them tries to claim. Only the conditional UPDATE can then pick a single winner.
    import app.service as service_module

    n = 10
    after_read_new = threading.Barrier(n, timeout=10)
    real_check = service_module.is_kill_switch_on

    def gated_check(conn):  # called by submit() right after it has seen status NEW
        after_read_new.wait()
        return real_check(conn)

    monkeypatch.setattr(service_module, "is_kill_switch_on", gated_check)
    results, errors = [], []

    def worker():
        try:
            results.append(service.submit(signal()))
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    assert broker.place_order_call_count == 1
    assert event_names(service).count("CLAIMED") == 1
    assert service.get("sig-1").submit_attempts == 1


def test_new_row_left_by_a_crash_is_resumed_on_resubmit(service, broker, db_path):
    # Simulate a crash between INSERT and the claim: the row is NEW, the broker was never called.
    with connection(db_path) as c:
        c.execute(
            "INSERT INTO orders (signal_id, client_order_id, symbol, side, qty, reference_price,"
            " status, created_at, updated_at)"
            " VALUES ('sig-1', 'CO-sig-1', 'AAPL', 'BUY', '2', '100', 'NEW', 't', 't')"
        )
    assert service.submit(signal()).status is S.ACCEPTED
    assert broker.orders_created == 1


# --- max order value ---------------------------------------------------------------------

def test_over_limit_is_rejected_and_never_reaches_broker(service, broker):
    order = service.submit(signal(qty="11", price="100"))  # 1100 > 1000
    assert order.status is S.REJECTED_RISK
    assert "1100" in order.reject_reason and "1000" in order.reject_reason
    assert broker.place_order_call_count == 0
    assert broker.get_order("CO-sig-1") is None


def test_at_limit_is_accepted(service, broker):
    order = service.submit(signal(qty="10", price="100"))  # exactly 1000
    assert order.status is S.ACCEPTED
    assert broker.orders_created == 1


def test_over_limit_by_a_fraction_is_rejected(service, broker):
    order = service.submit(signal(qty="10", price="100.001"))  # 1000.01
    assert order.status is S.REJECTED_RISK
    assert broker.place_order_call_count == 0


def test_risk_rejected_resubmit_stays_rejected(service, broker):
    service.submit(signal(qty="11"))
    assert service.submit(signal(qty="11")).status is S.REJECTED_RISK
    assert broker.place_order_call_count == 0


# --- kill switch -------------------------------------------------------------------------

def test_kill_switch_on_blocks_submission(service, broker, db_path):
    kill_switch(db_path, True)
    order = service.submit(signal())
    assert order.status is S.BLOCKED
    assert order.reject_reason == "kill switch is on"
    assert broker.place_order_call_count == 0
    assert event_names(service) == ["RECEIVED", "BLOCKED_KILL_SWITCH"]


def test_kill_switch_off_allows_submission(service, broker, db_path):
    kill_switch(db_path, True)
    kill_switch(db_path, False)
    assert service.submit(signal()).status is S.ACCEPTED
    assert broker.orders_created == 1


def test_blocked_signal_stays_blocked_after_kill_switch_off(service, broker, db_path):
    # BLOCKED is terminal by design: a stale signal must not fire later.
    kill_switch(db_path, True)
    service.submit(signal("sig-1"))
    kill_switch(db_path, False)
    assert service.submit(signal("sig-1")).status is S.BLOCKED
    assert service.submit(signal("sig-2")).status is S.ACCEPTED  # new signals go through
    assert broker.place_order_call_count == 1


def test_kill_switch_is_checked_before_risk_limit(service, broker, db_path):
    kill_switch(db_path, True)
    assert service.submit(signal(qty="11")).status is S.BLOCKED


def test_value_just_over_limit_is_not_rounded_down_to_the_limit(db_path, broker):
    # Valid inputs (24 digits, 10 decimals each) whose exact product is
    # 1000000000.00000000000000000002, just over the limit. Python's default 28-digit Decimal
    # context rounds it to exactly 1000000000, and the order would have passed.
    from app.config import Settings
    from app.service import OrderService

    limit = Decimal("1000000000")
    svc = OrderService(db_path, broker, Settings(db_path, broker.db_path, limit))
    order = svc.submit(signal(qty="97803337049860.1412280187", price="0.0000102246"))
    assert order.status is S.REJECTED_RISK
    assert broker.place_order_call_count == 0
