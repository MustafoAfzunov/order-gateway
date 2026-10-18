import sqlite3
from collections.abc import Iterator
from decimal import Decimal

import pytest

from app.broker import MockBroker
from app.config import Settings
from app.db import connection, init_schema
from app.service import OrderService

MAX_ORDER_VALUE = Decimal("1000")


@pytest.fixture
def db_path(tmp_path) -> str:
    # A real file, not ":memory:": several connections must see the same DB, and a
    # "restart" must be able to reopen it.
    path = str(tmp_path / "gateway.db")
    init_schema(path)
    return path


@pytest.fixture
def conn(db_path) -> Iterator[sqlite3.Connection]:
    with connection(db_path) as c:
        yield c


@pytest.fixture
def broker(tmp_path) -> MockBroker:
    return MockBroker(str(tmp_path / "broker.db"))


@pytest.fixture
def service(db_path, broker) -> OrderService:
    settings = Settings(db_path=db_path, broker_db_path=broker.db_path,
                        max_order_value=MAX_ORDER_VALUE)
    return OrderService(db_path, broker, settings)
