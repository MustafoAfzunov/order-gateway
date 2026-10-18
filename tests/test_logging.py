import logging
import re
from decimal import Decimal

import pytest

from app.broker import Behavior, BrokerStatus
from app.db import connection
from app.logging_setup import LOGGER_NAME, KeyValueFormatter, log_event
from tests.helpers import kill_switch, signal


class ListHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.setFormatter(KeyValueFormatter())
        self.lines: list[str] = []

    def emit(self, record):
        self.lines.append(self.format(record))


@pytest.fixture
def lines():
    logger = logging.getLogger(LOGGER_NAME)
    handler = ListHandler()
    logger.addHandler(handler)
    old_level = logger.level
    logger.setLevel(logging.INFO)
    yield handler.lines
    logger.removeHandler(handler)
    logger.setLevel(old_level)


def events_of(lines):
    return [re.search(r" event=(\S+)", line).group(1) for line in lines]


def test_formatter_renders_key_value_and_quotes_awkward_values(lines):
    log_event(logging.getLogger(LOGGER_NAME), "X", signal_id="s1", client_order_id=None,
              reason="has space", qty=Decimal("1.50"))
    [line] = lines
    assert re.match(r"ts=\S+ level=INFO event=X signal_id=s1 client_order_id=- ", line)
    assert 'reason="has space"' in line
    assert "qty=1.50" in line  # Decimal printed exactly


def test_log_event_requires_signal_and_client_order_ids():
    with pytest.raises(TypeError):
        log_event(logging.getLogger(LOGGER_NAME), "X")  # type: ignore[call-arg]


def test_every_line_carries_signal_id_and_client_order_id(service, broker, db_path, lines):
    # Drive every kind of event once.
    service.submit(signal("ok", qty="10", price="10"))
    service.submit(signal("ok", qty="10", price="10"))                    # duplicate
    service.submit(signal("big", qty="11"))                               # risk rejected
    broker.script("CO-lost", Behavior.ACCEPT_THEN_TIMEOUT)
    service.submit(signal("lost"))                                        # timeout + lookup
    u = broker.make_update("CO-ok", BrokerStatus.PARTIALLY_FILLED, Decimal("4"))
    service.handle_update(u)                                              # applied
    service.handle_update(u)                                              # ignored
    service.handle_update({"client_order_id": "CO-x", "seq": 1, "status": "FILLED",
                           "cumulative_filled_qty": "1"})                 # unknown order
    broker.make_update("CO-ok", BrokerStatus.FILLED, Decimal("10"))
    service.reconcile()                                                   # reconciled
    kill_switch(db_path, True)
    service.submit(signal("blocked"))                                     # blocked

    for line in lines:
        assert " signal_id=" in line and " client_order_id=" in line, line

    seen = set(events_of(lines))
    assert {"RECEIVED", "DUPLICATE", "REJECTED_RISK", "BLOCKED_KILL_SWITCH", "CLAIMED",
            "BROKER_SEND", "BROKER_RESPONSE", "BROKER_TIMEOUT", "BROKER_LOOKUP",
            "RESOLVED_FROM_BROKER", "BROKER_UPDATE_APPLIED", "BROKER_UPDATE_IGNORED",
            "RECONCILED", "RECONCILE_RESULT", "RECONCILE_DONE"} <= seen


def test_one_instruction_story_can_be_grepped(service, broker, lines):
    broker.script("CO-lost", Behavior.ACCEPT_THEN_TIMEOUT)
    service.submit(signal("lost"))
    service.submit(signal("other"))
    story = [line for line in lines if "signal_id=lost " in line]
    assert events_of(story) == ["RECEIVED", "CLAIMED", "BROKER_SEND", "BROKER_TIMEOUT",
                                "BROKER_LOOKUP", "RESOLVED_FROM_BROKER"]
    assert all("client_order_id=CO-lost" in line for line in story)


def test_lines_from_a_rolled_back_transaction_are_not_logged(service, db_path, lines):
    with connection(db_path) as conn:
        with pytest.raises(RuntimeError):
            with service._tx(conn) as pending:
                pending.append({"event": "SHOULD_NOT_APPEAR", "signal_id": "s",
                                "client_order_id": "c"})
                raise RuntimeError("boom")
    assert "SHOULD_NOT_APPEAR" not in events_of(lines)
