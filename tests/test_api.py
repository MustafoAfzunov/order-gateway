from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

from app.api import create_app
from app.broker import Behavior, BrokerStatus, BrokerTimeout, MockBroker
from app.config import Settings
from app.models import OrderStatus
from app.service import OrderService
from tests.helpers import signal

S = OrderStatus


@pytest.fixture
def settings(tmp_path) -> Settings:
    return Settings(db_path=str(tmp_path / "gateway.db"),
                    broker_db_path=str(tmp_path / "broker.db"),
                    max_order_value=Decimal("1000"))


@pytest.fixture
def client(settings):
    with TestClient(create_app(settings)) as c:  # `with` runs the lifespan (startup reconcile)
        yield c


def body(signal_id="sig-1", qty="2", price="100", side="BUY", instrument="AAPL"):
    return {"signal_id": signal_id, "instrument": instrument, "side": side, "qty": qty,
            "reference_price": price}


def test_post_signal_returns_accepted_order_with_decimals_as_strings(client):
    r = client.post("/signals", json=body())
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "ACCEPTED"
    assert data["client_order_id"] == "CO-sig-1"
    assert data["qty"] == "2"  # Decimal serialised as a string, never a float


def test_duplicate_post_returns_same_order_and_one_broker_order(client):
    first = client.post("/signals", json=body()).json()
    second = client.post("/signals", json=body()).json()
    assert second["id"] == first["id"]
    assert client.app.state.broker.orders_created == 1


def test_same_signal_id_different_payload_is_409(client):
    client.post("/signals", json=body())
    r = client.post("/signals", json=body(qty="3"))
    assert r.status_code == 409


@pytest.mark.parametrize("bad", [{"qty": "0"}, {"reference_price": "-1"}, {"side": "HOLD"},
                                 {"signal_id": "has space"}])
def test_invalid_signal_is_422(client, bad):
    assert client.post("/signals", json={**body(), **bad}).status_code == 422


def test_over_limit_signal_is_recorded_as_rejected_risk(client):
    data = client.post("/signals", json=body(qty="11")).json()  # 1100 > 1000
    assert data["status"] == "REJECTED_RISK"
    assert client.app.state.broker.place_order_call_count == 0


def test_get_order_returns_order_and_its_event_log(client):
    client.post("/signals", json=body())
    data = client.get("/orders/sig-1").json()
    assert data["order"]["status"] == "ACCEPTED"
    assert [e["event"] for e in data["events"]] == ["RECEIVED", "CLAIMED", "BROKER_RESPONSE"]


def test_get_unknown_order_is_404(client):
    assert client.get("/orders/nope").status_code == 404


def test_broker_update_applied_then_replay_ignored(client):
    client.post("/signals", json=body(qty="10", price="10"))
    update = client.app.state.broker.make_update("CO-sig-1", BrokerStatus.PARTIALLY_FILLED,
                                                 Decimal("4"))
    first = client.post("/broker/updates", json=update).json()
    replay = client.post("/broker/updates", json=update)

    assert first["applied"] is True and first["order"]["filled_qty"] == "4"
    assert replay.status_code == 200
    assert replay.json()["applied"] is False
    assert replay.json()["reason"] == "STALE_OR_DUPLICATE"


def test_broker_update_for_unknown_order_is_200_and_ignored(client):
    r = client.post("/broker/updates", json={"client_order_id": "CO-x", "seq": 1,
                                             "status": "FILLED", "cumulative_filled_qty": "1"})
    assert r.status_code == 200
    assert r.json() == {"applied": False, "reason": "UNKNOWN_ORDER", "order": None}


def test_malformed_broker_update_is_422(client):
    r = client.post("/broker/updates", json={"client_order_id": "CO-x", "seq": 0,
                                             "status": "FILLED", "cumulative_filled_qty": "1"})
    assert r.status_code == 422


def test_kill_switch_blocks_signals_but_reads_updates_and_reconcile_work(client):
    client.post("/signals", json=body("live", qty="10", price="10"))
    assert client.post("/kill-switch", json={"on": True}).json() == {"on": True}

    assert client.post("/signals", json=body("new")).json()["status"] == "BLOCKED"
    assert client.get("/orders/live").status_code == 200
    update = client.app.state.broker.make_update("CO-live", BrokerStatus.FILLED, Decimal("10"))
    assert client.post("/broker/updates", json=update).json()["applied"] is True
    assert client.post("/reconcile").status_code == 200
    assert client.app.state.broker.place_order_call_count == 1  # only the pre-switch order

    assert client.post("/kill-switch", json={"on": False}).json() == {"on": False}
    assert client.post("/signals", json=body("after")).json()["status"] == "ACCEPTED"


def test_reconcile_endpoint_reports_what_changed(client):
    client.post("/signals", json=body(qty="10", price="10"))
    # The broker fills the order, but the push never reaches us.
    client.app.state.broker.make_update("CO-sig-1", BrokerStatus.FILLED, Decimal("10"))
    assert client.post("/reconcile").json() == [{
        "signal_id": "sig-1", "status_before": "ACCEPTED", "status_after": "FILLED",
        "note": "updated from broker"}]


def test_startup_reconciles_orders_left_unresolved(settings, monkeypatch):
    # Before "this" process starts: a lost response + failed lookup left an UNKNOWN order.
    from app.db import init_schema
    init_schema(settings.db_path)
    old_broker = MockBroker(settings.broker_db_path)
    old_broker.script("CO-sig-1", Behavior.ACCEPT_THEN_TIMEOUT)

    def lookup_times_out(coid):
        raise BrokerTimeout(coid)

    monkeypatch.setattr(old_broker, "get_order", lookup_times_out)
    old = OrderService(settings.db_path, old_broker, settings)
    assert old.submit(signal()).status is S.UNKNOWN

    with TestClient(create_app(settings)) as client:  # startup runs reconcile()
        assert client.get("/orders/sig-1").json()["order"]["status"] == "ACCEPTED"
        assert client.app.state.broker.place_order_call_count == 0
        assert client.app.state.broker.orders_created == 1


def test_kill_switch_stays_on_across_restart(settings):
    with TestClient(create_app(settings)) as client:
        client.post("/kill-switch", json={"on": True})
    with TestClient(create_app(settings)) as client:  # new process, same DB file
        assert client.post("/signals", json=body()).json()["status"] == "BLOCKED"
        assert client.app.state.broker.place_order_call_count == 0


@pytest.mark.parametrize("limit, expected", [("200", "ACCEPTED"), ("199.99", "REJECTED_RISK")])
def test_max_order_value_is_configurable_via_env(tmp_path, monkeypatch, limit, expected):
    # End to end, as in production: no Settings object passed in; the app reads env at startup.
    monkeypatch.setenv("DB_PATH", str(tmp_path / "gateway.db"))
    monkeypatch.setenv("BROKER_DB_PATH", str(tmp_path / "broker.db"))
    monkeypatch.setenv("MAX_ORDER_VALUE", limit)
    with TestClient(create_app()) as client:
        r = client.post("/signals", json=body(qty="2", price="100"))  # value 200
        assert r.json()["status"] == expected
