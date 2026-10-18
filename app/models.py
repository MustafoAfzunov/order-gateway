"""Pydantic schemas, order statuses and the state machine."""

from decimal import Decimal
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class Side(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class OrderStatus(StrEnum):
    NEW = "NEW"
    SUBMITTING = "SUBMITTING"
    UNKNOWN = "UNKNOWN"
    ACCEPTED = "ACCEPTED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    REJECTED = "REJECTED"  # rejected by the broker
    # Set by us, before the broker is ever called:
    BLOCKED = "BLOCKED"  # kill switch was on when the signal arrived
    REJECTED_RISK = "REJECTED_RISK"  # qty * reference_price > MAX_ORDER_VALUE


S = OrderStatus

# BLOCKED is terminal on purpose: signals are time-sensitive, so a signal blocked by the kill
# switch must not fire later when the switch is turned off. The sender has to send a new one.
TERMINAL: frozenset[OrderStatus] = frozenset({S.FILLED, S.REJECTED, S.BLOCKED, S.REJECTED_RISK})

ALLOWED_TRANSITIONS: dict[OrderStatus, frozenset[OrderStatus]] = {
    # The atomic claim, or one of our own pre-broker checks. Nothing else leaves NEW.
    S.NEW: frozenset({S.SUBMITTING, S.BLOCKED, S.REJECTED_RISK}),
    # The broker answered, or it didn't (timeout/crash) and we don't know if it has the order.
    S.SUBMITTING: frozenset({S.ACCEPTED, S.PARTIALLY_FILLED, S.FILLED, S.REJECTED, S.UNKNOWN}),
    # get_order() found it, so take the broker's state. Or it said "not found", and only then
    # may we re-claim (UNKNOWN -> SUBMITTING) and send again with the same client_order_id.
    S.UNKNOWN: frozenset({S.ACCEPTED, S.PARTIALLY_FILLED, S.FILLED, S.REJECTED, S.SUBMITTING}),
    S.ACCEPTED: frozenset({S.PARTIALLY_FILLED, S.FILLED, S.REJECTED}),
    # Self-transition: another partial fill raises cumulative filled_qty.
    S.PARTIALLY_FILLED: frozenset({S.PARTIALLY_FILLED, S.FILLED}),
    # Terminal states never change.
    S.FILLED: frozenset(),
    S.REJECTED: frozenset(),
    S.BLOCKED: frozenset(),
    S.REJECTED_RISK: frozenset(),
}


def can_transition(current: OrderStatus, new: OrderStatus) -> bool:
    return new in ALLOWED_TRANSITIONS[current]


def client_order_id_for(signal_id: str) -> str:
    # Deterministic, so every retry, restart and reconciliation asks the broker about the
    # same order. A random ID would make a lost response impossible to look up.
    return f"CO-{signal_id}"


class Signal(BaseModel):
    # extra="forbid": a typo in a field name is an error, not a silently dropped field.
    model_config = ConfigDict(extra="forbid", frozen=True)

    # Restricted characters because signal_id ends up inside client_order_id sent to the broker.
    signal_id: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_.:-]+$")
    symbol: str = Field(min_length=1, max_length=32)
    side: Side
    # Decimal rejects NaN and Infinity by default in Pydantic v2; gt=0 rejects zero/negative.
    # max_digits bounds the input, so the risk check's multiplication can be done exactly
    # (see risk.order_value) and nobody can send a megabyte-long number.
    qty: Decimal = Field(gt=0, max_digits=24, decimal_places=10)
    reference_price: Decimal = Field(gt=0, max_digits=24, decimal_places=10)

    @property
    def client_order_id(self) -> str:
        return client_order_id_for(self.signal_id)


class Order(BaseModel):
    """Read-only view of one row in the orders table."""

    id: int
    signal_id: str
    client_order_id: str
    symbol: str
    side: Side
    qty: Decimal
    reference_price: Decimal
    status: OrderStatus
    filled_qty: Decimal
    last_seq: int
    submit_attempts: int
    broker_order_id: str | None
    reject_reason: str | None
    created_at: str
    updated_at: str

    @classmethod
    def from_row(cls, row: Any) -> "Order":
        return cls(**dict(row))  # Pydantic parses the TEXT columns back into Decimal


# Statuses a broker update may carry. The rest (NEW, SUBMITTING, UNKNOWN, BLOCKED,
# REJECTED_RISK) only exist on our side.
BROKER_REPORTED = frozenset({S.ACCEPTED, S.REJECTED, S.PARTIALLY_FILLED, S.FILLED})


class BrokerUpdate(BaseModel):
    """One push message from the broker about one order."""

    model_config = ConfigDict(frozen=True)

    client_order_id: str = Field(min_length=1)
    broker_order_id: str | None = None
    seq: int = Field(ge=1)  # per-order, increases with every change at the broker
    status: OrderStatus
    # CUMULATIVE filled qty, not a delta. That makes a replay harmless (same total) and a
    # skipped seq harmless (the next total already includes the missed fill).
    cumulative_filled_qty: Decimal = Field(ge=0)

    @field_validator("status")
    @classmethod
    def _broker_status_only(cls, v: OrderStatus) -> OrderStatus:
        if v not in BROKER_REPORTED:
            raise ValueError(f"{v} is not a status the broker reports")
        return v
