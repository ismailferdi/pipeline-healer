import re

import pytest

from pipeline_healer.pipeline.producers.furniture import FURNITURE_CATALOG, FURNITURE_ITEMS
from pipeline_healer.pipeline.producers.order_events import generate_order_events

SKU_PATTERN = re.compile(r"^sku_(liv|bed|din|kit|off|kid|bat|sto|tex|lig|dec|out)_\d{3}$")
PRICE_BY_SKU = {sku: unit_price for sku, _, unit_price in FURNITURE_ITEMS}
NAME_BY_SKU = {sku: name for sku, name, _ in FURNITURE_ITEMS}


def test_catalog_covers_every_home_area() -> None:
    assert set(FURNITURE_CATALOG) == {
        "living_room",
        "bedroom",
        "dining",
        "kitchen",
        "office",
        "kids",
        "bathroom",
        "storage",
        "textiles",
        "lighting",
        "decor",
        "outdoor",
    }
    assert len(FURNITURE_ITEMS) == 85
    assert all(items for items in FURNITURE_CATALOG.values())


def test_skus_and_names_are_unique() -> None:
    skus = [sku for sku, _, _ in FURNITURE_ITEMS]
    names = [name for _, name, _ in FURNITURE_ITEMS]

    assert len(skus) == len(set(skus))
    assert len(names) == len(set(names))
    assert all(SKU_PATTERN.match(sku) for sku in skus)


@pytest.mark.parametrize(("sku", "name", "price"), FURNITURE_ITEMS)
def test_prices_are_positive_and_realistic(sku: str, name: str, price: float) -> None:
    assert 1_000 <= price <= 300_000
    assert any("\u0600" <= char <= "\u06ff" for char in name)


def test_order_generator_uses_only_catalog_items() -> None:
    for event in generate_order_events(200, seed=9):
        if event["event_type"] != "order_placed":
            continue

        for item in event["payload"]["items"]:
            assert item["sku"] in PRICE_BY_SKU
            assert item["name"] == NAME_BY_SKU[item["sku"]]
            assert item["unit_price"] == PRICE_BY_SKU[item["sku"]]
            assert 1 <= item["quantity"] <= 2
