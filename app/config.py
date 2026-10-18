"""Settings read from environment variables."""

import os
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation


@dataclass(frozen=True)
class Settings:
    db_path: str
    broker_db_path: str
    max_order_value: Decimal


def load_settings() -> Settings:
    # Read on every call (not at import time) so tests can set env vars per test.
    raw_max = os.environ.get("MAX_ORDER_VALUE", "100000")
    try:
        # Parse from the string, never via float: float("0.1") is not 0.1.
        max_order_value = Decimal(raw_max)
    except InvalidOperation:
        raise ValueError(f"MAX_ORDER_VALUE is not a number: {raw_max!r}") from None
    if not max_order_value.is_finite() or max_order_value <= 0:
        raise ValueError(f"MAX_ORDER_VALUE must be a positive number: {raw_max!r}")

    return Settings(
        db_path=os.environ.get("DB_PATH", "gateway.db"),
        broker_db_path=os.environ.get("BROKER_DB_PATH", "broker.db"),
        max_order_value=max_order_value,
    )
