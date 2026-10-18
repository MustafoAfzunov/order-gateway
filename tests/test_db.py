import sqlite3
from decimal import Decimal

import pytest

from app.db import connection, init_schema
from app.models import OrderStatus

NOW = "2026-01-01T00:00:00+00:00"


def insert_order(conn, signal_id="sig-1", client_order_id=None, side="BUY",
                 status="NEW", qty="10", price="100.5"):
    cur = conn.execute(
        "INSERT INTO orders (signal_id, client_order_id, instrument, side, qty, reference_price,"
        " status, created_at, updated_at) VALUES (?, ?, 'AAPL', ?, ?, ?, ?, ?, ?)",
        (signal_id, client_order_id or f"CO-{signal_id}", side, qty, price, status, NOW, NOW),
    )
    return cur.lastrowid


def test_init_schema_creates_tables(conn):
    names = {r["name"] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"orders", "order_events", "settings"} <= names


def test_init_schema_is_idempotent_and_keeps_data(db_path, conn):
    insert_order(conn)
    conn.execute("UPDATE settings SET value='on' WHERE key='kill_switch'")

    init_schema(db_path)  # second start-up

    assert conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0] == 1
    # The seed row must not reset a kill switch the operator turned on.
    assert conn.execute("SELECT value FROM settings WHERE key='kill_switch'").fetchone()[0] == "on"


def test_connection_uses_wal_mode(conn):
    assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"


def test_connection_enforces_foreign_keys(conn):
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO order_events (order_id, event, created_at) VALUES (999, 'X', ?)", (NOW,)
        )


def test_kill_switch_defaults_off(conn):
    assert conn.execute("SELECT value FROM settings WHERE key='kill_switch'").fetchone()[0] == "off"


def test_duplicate_signal_id_is_rejected_by_db(conn):
    insert_order(conn, signal_id="sig-1", client_order_id="CO-sig-1")
    with pytest.raises(sqlite3.IntegrityError):
        insert_order(conn, signal_id="sig-1", client_order_id="CO-other")


def test_duplicate_client_order_id_is_rejected_by_db(conn):
    insert_order(conn, signal_id="sig-1", client_order_id="CO-x")
    with pytest.raises(sqlite3.IntegrityError):
        insert_order(conn, signal_id="sig-2", client_order_id="CO-x")


def test_status_check_rejects_value_not_in_enum(conn):
    with pytest.raises(sqlite3.IntegrityError):
        insert_order(conn, status="CANCELLED")


@pytest.mark.parametrize("status", list(OrderStatus))
def test_status_check_accepts_every_enum_value(conn, status):
    # Keeps the SQL CHECK list and the Python enum in sync.
    insert_order(conn, status=status.value)


def test_side_check_rejects_invalid_side(conn):
    with pytest.raises(sqlite3.IntegrityError):
        insert_order(conn, side="HOLD")


def test_decimal_roundtrips_exactly_as_text(conn):
    insert_order(conn, qty="0.1", price="12345678901234567890.123456789")
    row = conn.execute("SELECT qty, reference_price FROM orders").fetchone()
    assert Decimal(row["qty"]) * 3 == Decimal("0.3")  # would fail with REAL/float
    assert Decimal(row["reference_price"]) == Decimal("12345678901234567890.123456789")


def test_state_survives_reopen(db_path):
    with connection(db_path) as c:
        insert_order(c, status="UNKNOWN")
    init_schema(db_path)  # simulated restart
    with connection(db_path) as c:
        assert c.execute("SELECT status FROM orders").fetchone()[0] == "UNKNOWN"


def test_write_is_visible_to_other_connection_immediately(db_path):
    # Autocommit mode: no hidden open transaction holding the write back.
    with connection(db_path) as a, connection(db_path) as b:
        insert_order(a)
        assert b.execute("SELECT COUNT(*) FROM orders").fetchone()[0] == 1
