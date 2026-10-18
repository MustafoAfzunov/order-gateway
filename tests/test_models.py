from decimal import Decimal

import pytest
from pydantic import ValidationError

from app.models import Side, Signal

VALID = {"signal_id": "sig-1", "symbol": "AAPL", "side": "BUY", "qty": "10",
         "reference_price": "100.25"}


def make(**overrides):
    return Signal(**{**VALID, **overrides})


def test_valid_signal_parses_decimals():
    s = make()
    assert s.side is Side.BUY
    assert s.qty == Decimal("10")
    assert s.reference_price == Decimal("100.25")


def test_client_order_id_is_derived_from_signal_id():
    assert make(signal_id="abc-42").client_order_id == "CO-abc-42"


@pytest.mark.parametrize("field", ["qty", "reference_price"])
@pytest.mark.parametrize("value", ["0", "-1", "-0.0001"])
def test_non_positive_qty_or_price_is_rejected(field, value):
    with pytest.raises(ValidationError):
        make(**{field: value})


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-Infinity", "abc"])
def test_non_finite_or_non_numeric_qty_is_rejected(value):
    with pytest.raises(ValidationError):
        make(qty=value)


@pytest.mark.parametrize("side", ["buy", "HOLD", ""])
def test_invalid_side_is_rejected(side):
    with pytest.raises(ValidationError):
        make(side=side)


@pytest.mark.parametrize("signal_id", ["", "has space", "a/b", "x" * 65])
def test_invalid_signal_id_is_rejected(signal_id):
    with pytest.raises(ValidationError):
        make(signal_id=signal_id)


def test_unknown_field_is_rejected():
    with pytest.raises(ValidationError):
        make(quantity="5")


def test_missing_field_is_rejected():
    data = dict(VALID)
    del data["reference_price"]
    with pytest.raises(ValidationError):
        Signal(**data)


@pytest.mark.parametrize("field, value", [("qty", "1" * 25), ("qty", "0.00000000001"),
                                          ("reference_price", "1" * 25)])
def test_too_many_digits_is_rejected(field, value):
    with pytest.raises(ValidationError):
        make(**{field: value})
