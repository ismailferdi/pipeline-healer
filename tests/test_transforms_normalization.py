import pytest

from pipeline_healer.pipeline.transforms.errors import TransformError
from pipeline_healer.pipeline.transforms.normalization import (
    normalize_currency,
    normalize_event_type,
    normalize_field_name,
    normalize_keys,
    normalize_text,
)
from pipeline_healer.pipeline.transforms.nulls import is_null, normalize_nulls, nullify


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("amount", "amount"),
        ("totalAmount", "total_amount"),
        ("Total Amount", "total_amount"),
        ("  total-amount  ", "total_amount"),
        ("__event__id__", "event_id"),
        ("fareAmountSAR", "fare_amount_sar"),
    ],
)
def test_normalize_field_name(raw: str, expected: str) -> None:
    assert normalize_field_name(raw) == expected


@pytest.mark.parametrize("raw", ["", "   ", "---", 42])
def test_normalize_field_name_rejects_empty_or_non_string(raw) -> None:
    with pytest.raises(TransformError):
        normalize_field_name(raw)


def test_normalize_keys_is_recursive() -> None:
    record = {"eventId": "evt_1", "payload": {"totalAmount": 10, "items": [{"unitPrice": 2}]}}

    assert normalize_keys(record) == {
        "event_id": "evt_1",
        "payload": {"total_amount": 10, "items": [{"unit_price": 2}]},
    }


def test_normalize_keys_rejects_collisions() -> None:
    with pytest.raises(TransformError, match="duplicate field"):
        normalize_keys({"totalAmount": 1, "total_amount": 2})


def test_normalize_keys_rejects_non_string_keys() -> None:
    with pytest.raises(TransformError, match="must be strings"):
        normalize_keys({1: "value"})


def test_normalize_text_handles_unicode_and_whitespace() -> None:
    assert normalize_text("  قهوة   عربية  ") == "قهوة عربية"
    assert normalize_text("e\u0301") == "\u00e9"


def test_normalize_event_type_and_currency() -> None:
    assert normalize_event_type(" Ride-Completed ") == "ride_completed"
    assert normalize_event_type("ORDER PLACED") == "order_placed"
    assert normalize_currency(" dzd ") == "DZD"

    with pytest.raises(TransformError):
        normalize_event_type("")
    with pytest.raises(TransformError):
        normalize_currency("   ")


@pytest.mark.parametrize(
    "value", [None, "", " ", "null", "NULL", "None", "N/A", "n/a", "-", "غير متوفر"]
)
def test_null_tokens(value) -> None:
    assert is_null(value)


@pytest.mark.parametrize("value", [0, False, "0", "قيمة", "— غير متوفر"])
def test_non_null_values(value) -> None:
    assert not is_null(value)


def test_nullify_and_normalize_nulls() -> None:
    assert nullify(" N/A ") is None
    assert nullify("قيمة") == "قيمة"

    record = {"city": "الجزائر", "note": " null ", "items": [{"reason": "N/A", "qty": 2}]}
    assert normalize_nulls(record) == {
        "city": "الجزائر",
        "note": None,
        "items": [{"reason": None, "qty": 2}],
    }
