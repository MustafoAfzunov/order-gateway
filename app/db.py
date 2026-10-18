"""SQLite connection helper and schema."""

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager

# Quantities and prices are TEXT holding a Decimal string. SQLite has no decimal type, and
# REAL would silently round money.
SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
    id               INTEGER PRIMARY KEY,
    -- UNIQUE is what makes duplicate submissions safe: the DB, not Python, decides
    -- which insert wins.
    signal_id        TEXT NOT NULL UNIQUE,
    client_order_id  TEXT NOT NULL UNIQUE,
    instrument       TEXT NOT NULL,
    side             TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
    qty              TEXT NOT NULL,
    reference_price  TEXT NOT NULL,
    status           TEXT NOT NULL CHECK (status IN (
                         'NEW', 'SUBMITTING', 'UNKNOWN', 'ACCEPTED',
                         'PARTIALLY_FILLED', 'FILLED', 'REJECTED',
                         'BLOCKED', 'REJECTED_RISK')),
    filled_qty       TEXT NOT NULL DEFAULT '0',   -- cumulative, as reported by the broker
    last_seq         INTEGER NOT NULL DEFAULT 0,  -- highest broker update seq applied
    submit_attempts  INTEGER NOT NULL DEFAULT 0,
    broker_order_id  TEXT,
    reject_reason    TEXT,
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS order_events (
    id           INTEGER PRIMARY KEY,
    order_id     INTEGER NOT NULL REFERENCES orders(id),
    event        TEXT NOT NULL,
    from_status  TEXT,
    to_status    TEXT,
    detail       TEXT,                            -- JSON
    created_at   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS order_events_order_id ON order_events(order_id);

CREATE TABLE IF NOT EXISTS settings (
    key    TEXT PRIMARY KEY,
    value  TEXT NOT NULL
);
-- OR IGNORE: on restart, keep whatever the operator set; only seed a fresh DB.
INSERT OR IGNORE INTO settings (key, value) VALUES ('kill_switch', 'off');
"""


def connect(path: str) -> sqlite3.Connection:
    # isolation_level=None turns off Python's implicit BEGIN. Each statement commits on
    # its own unless we write BEGIN explicitly, so transaction boundaries are visible in
    # our code instead of hidden in the driver.
    conn = sqlite3.connect(path, isolation_level=None, timeout=5.0)
    conn.row_factory = sqlite3.Row
    # WAL lets readers run while one writer writes; busy_timeout makes concurrent
    # writers wait for the lock instead of failing at once with "database is locked".
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA foreign_keys=ON")  # SQLite leaves FK checks off by default
    return conn


@contextmanager
def connection(path: str) -> Iterator[sqlite3.Connection]:
    """One short-lived connection per call. sqlite3 connections must not be shared across
    threads, and FastAPI runs sync endpoints in a thread pool."""
    conn = connect(path)
    try:
        yield conn
    finally:
        conn.close()


def init_schema(path: str) -> None:
    with connection(path) as conn:
        conn.executescript(SCHEMA)
