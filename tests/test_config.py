from decimal import Decimal

import pytest

from app.config import load_settings


def test_defaults_when_env_unset(monkeypatch):
    for name in ("DB_PATH", "BROKER_DB_PATH", "MAX_ORDER_VALUE"):
        monkeypatch.delenv(name, raising=False)
    s = load_settings()
    assert s.db_path == "gateway.db"
    assert s.broker_db_path == "broker.db"
    assert s.max_order_value == Decimal("100000")


def test_env_vars_override_defaults(monkeypatch):
    monkeypatch.setenv("DB_PATH", "/tmp/g.db")
    monkeypatch.setenv("BROKER_DB_PATH", "/tmp/b.db")
    monkeypatch.setenv("MAX_ORDER_VALUE", "2500.75")
    s = load_settings()
    assert (s.db_path, s.broker_db_path) == ("/tmp/g.db", "/tmp/b.db")
    assert s.max_order_value == Decimal("2500.75")


@pytest.mark.parametrize("value", ["abc", "0", "-5", "NaN", "Infinity"])
def test_invalid_max_order_value_fails_at_startup(monkeypatch, value):
    monkeypatch.setenv("MAX_ORDER_VALUE", value)
    with pytest.raises(ValueError):
        load_settings()
