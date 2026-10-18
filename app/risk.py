"""Pre-trade checks: the max order value limit and the kill switch."""

import sqlite3
from decimal import Decimal, Inexact, localcontext


def order_value(qty: Decimal, reference_price: Decimal) -> Decimal:
    # Decimal arithmetic rounds to the context precision (28 significant digits by default),
    # so 1000.0000000000000000000000000001 * 1 would come out as exactly 1000 and pass a
    # 1000 limit. Signal caps inputs at 24 digits each, so a product has at most 48: with
    # precision 60 it is always exact. The Inexact trap turns any rounding into an error
    # instead of a silently wrong risk decision.
    with localcontext() as ctx:
        ctx.prec = 60
        ctx.traps[Inexact] = True
        return qty * reference_price


def max_value_violation(qty: Decimal, reference_price: Decimal, max_value: Decimal) -> str | None:
    """Return a human-readable reason if the order is too big, else None.
    The limit is inclusive: an order worth exactly max_value is allowed."""
    value = order_value(qty, reference_price)
    if value > max_value:
        return f"order value {value} exceeds MAX_ORDER_VALUE {max_value}"
    return None


# The kill switch lives in the DB, not in memory, so it survives a restart and every
# worker/thread sees the same value.
def is_kill_switch_on(conn: sqlite3.Connection) -> bool:
    row = conn.execute("SELECT value FROM settings WHERE key = 'kill_switch'").fetchone()
    return row is not None and row["value"] == "on"


def set_kill_switch(conn: sqlite3.Connection, on: bool) -> None:
    conn.execute(
        "INSERT INTO settings (key, value) VALUES ('kill_switch', ?)"
        " ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        ("on" if on else "off",),
    )
