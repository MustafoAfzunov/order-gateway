"""Mock broker. It stands in for an external system, so it has its own SQLite file and knows
nothing about the gateway's tables. Its state survives the gateway restarting, just like a
real broker's would."""

import threading
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Any

from app.db import connection

BROKER_SCHEMA = """
CREATE TABLE IF NOT EXISTS broker_orders (
    -- PRIMARY KEY on client_order_id is what makes place_order idempotent.
    client_order_id  TEXT PRIMARY KEY,
    broker_order_id  TEXT NOT NULL UNIQUE,
    instrument       TEXT NOT NULL,
    side             TEXT NOT NULL,
    qty              TEXT NOT NULL,
    price            TEXT NOT NULL,
    status           TEXT NOT NULL,
    filled_qty       TEXT NOT NULL,
    seq              INTEGER NOT NULL
);
"""


class Behavior(StrEnum):
    ACCEPT = "ACCEPT"
    REJECT = "REJECT"
    PARTIAL_FILL = "PARTIAL_FILL"
    FILL = "FILL"
    # Stores the order, then raises: "the broker accepted it but the response was lost".
    ACCEPT_THEN_TIMEOUT = "ACCEPT_THEN_TIMEOUT"
    # Raises without storing: "the request never reached the broker". get_order() then
    # returns None, which is the only case where the gateway may send again.
    TIMEOUT_BEFORE_ACCEPT = "TIMEOUT_BEFORE_ACCEPT"


class BrokerStatus(StrEnum):
    ACCEPTED = "ACCEPTED"
    REJECTED = "REJECTED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"


class BrokerTimeout(Exception):
    """The gateway can't tell whether the broker has the order."""


class BrokerOrderNotFound(Exception):
    pass


@dataclass(frozen=True)
class BrokerOrder:
    client_order_id: str
    broker_order_id: str
    instrument: str
    side: str
    qty: Decimal
    price: Decimal
    status: BrokerStatus
    filled_qty: Decimal  # cumulative
    seq: int  # bumps on every change; the gateway ignores anything it has already seen


def _row_to_order(row: Any) -> BrokerOrder:
    return BrokerOrder(
        client_order_id=row["client_order_id"],
        broker_order_id=row["broker_order_id"],
        instrument=row["instrument"],
        side=row["side"],
        qty=Decimal(row["qty"]),
        price=Decimal(row["price"]),
        status=BrokerStatus(row["status"]),
        filled_qty=Decimal(row["filled_qty"]),
        seq=row["seq"],
    )


class MockBroker:
    def __init__(self, db_path: str, default_behavior: Behavior = Behavior.ACCEPT) -> None:
        self.db_path = db_path
        self.default_behavior = default_behavior
        self._scripted: defaultdict[str, deque[Behavior]] = defaultdict(deque)
        self._lock = threading.Lock()  # guards the in-memory counter and scripts
        self.place_order_call_count = 0  # in-memory: resets when a new MockBroker is built
        with connection(db_path) as conn:
            conn.executescript(BROKER_SCHEMA)

    def script(self, client_order_id: str, *behaviors: Behavior) -> None:
        """Queue behaviors for one order. Each place_order call that has to create the order
        uses the next one; when the queue is empty, default_behavior applies."""
        with self._lock:
            self._scripted[client_order_id].extend(behaviors)

    @property
    def orders_created(self) -> int:
        # Counted from the DB, so it's still correct across restarts and threads.
        with connection(self.db_path) as conn:
            return int(conn.execute("SELECT COUNT(*) FROM broker_orders").fetchone()[0])

    def place_order(
        self,
        client_order_id: str,
        instrument: str,
        side: str,
        qty: Decimal,
        price: Decimal,
        behavior: Behavior | None = None,
    ) -> BrokerOrder:
        with self._lock:
            self.place_order_call_count += 1

        # Idempotency: an existing order is returned as-is. No behavior applies, so a resend
        # can neither change the order nor time out a second time.
        existing = self._load(client_order_id)
        if existing is not None:
            return existing

        behavior = behavior or self._next_behavior(client_order_id)
        if behavior is Behavior.TIMEOUT_BEFORE_ACCEPT:
            raise BrokerTimeout(client_order_id)

        status, filled = {
            Behavior.ACCEPT: (BrokerStatus.ACCEPTED, Decimal("0")),
            Behavior.ACCEPT_THEN_TIMEOUT: (BrokerStatus.ACCEPTED, Decimal("0")),
            Behavior.REJECT: (BrokerStatus.REJECTED, Decimal("0")),
            Behavior.PARTIAL_FILL: (BrokerStatus.PARTIALLY_FILLED, qty / 2),
            Behavior.FILL: (BrokerStatus.FILLED, qty),
        }[behavior]

        with connection(self.db_path) as conn:
            # The get_order() check above is only a shortcut. Two concurrent calls can both
            # miss it, so ON CONFLICT DO NOTHING is the real guard: the first insert wins
            # and the loser reads the winner's row below.
            conn.execute(
                "INSERT INTO broker_orders (client_order_id, broker_order_id, instrument, side,"
                " qty, price, status, filled_qty, seq) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)"
                " ON CONFLICT(client_order_id) DO NOTHING",
                (client_order_id, f"BRK-{uuid.uuid4().hex[:12]}", instrument, side,
                 str(qty), str(price), status.value, str(filled)),
            )
            row = conn.execute(
                "SELECT * FROM broker_orders WHERE client_order_id = ?", (client_order_id,)
            ).fetchone()

        if behavior is Behavior.ACCEPT_THEN_TIMEOUT:
            raise BrokerTimeout(client_order_id)  # the order IS stored; the caller doesn't know
        return _row_to_order(row)

    def get_order(self, client_order_id: str) -> BrokerOrder | None:
        return self._load(client_order_id)

    def _load(self, client_order_id: str) -> BrokerOrder | None:
        # Internal reads go through here, not get_order(), so tests that spy on get_order()
        # see only the gateway's own lookups.
        with connection(self.db_path) as conn:
            row = conn.execute(
                "SELECT * FROM broker_orders WHERE client_order_id = ?", (client_order_id,)
            ).fetchone()
        return _row_to_order(row) if row else None

    def make_update(
        self, client_order_id: str, status: BrokerStatus, cumulative_filled_qty: Decimal
    ) -> dict[str, Any]:
        """Change the order at the broker and return the update it would push to the gateway.
        Tests can deliver the result twice or out of order to simulate a misbehaving feed."""
        with connection(self.db_path) as conn:
            # BEGIN IMMEDIATE takes the write lock up front, so the read, the bounds check and
            # the seq bump happen together and two updates can never get the same seq.
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    "SELECT qty FROM broker_orders WHERE client_order_id = ?", (client_order_id,)
                ).fetchone()
                if row is None:
                    raise BrokerOrderNotFound(client_order_id)
                if not Decimal("0") <= cumulative_filled_qty <= Decimal(row["qty"]):
                    raise ValueError(
                        f"cumulative_filled_qty {cumulative_filled_qty} outside 0..{row['qty']}"
                    )
                updated = conn.execute(
                    "UPDATE broker_orders SET status = ?, filled_qty = ?, seq = seq + 1"
                    " WHERE client_order_id = ? RETURNING broker_order_id, seq",
                    (status.value, str(cumulative_filled_qty), client_order_id),
                ).fetchone()
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise

        return {
            "client_order_id": client_order_id,
            "broker_order_id": updated["broker_order_id"],
            "seq": updated["seq"],
            "status": status.value,
            # A string, not a float, so it can go through JSON without losing precision.
            "cumulative_filled_qty": str(cumulative_filled_qty),
        }

    def _next_behavior(self, client_order_id: str) -> Behavior:
        with self._lock:
            queue = self._scripted.get(client_order_id)
            return queue.popleft() if queue else self.default_behavior
