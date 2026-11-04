import pytest

from pipeline_healer.pipeline.producers.locations import WILAYAS
from pipeline_healer.pipeline.producers.order_events import generate_order_events
from pipeline_healer.pipeline.producers.ride_events import generate_ride_events

NEW_2025_WILAYAS = (
    "أفلو",
    "بريكة",
    "قصر الشلالة",
    "مسعد",
    "عين وسارة",
    "بوسعادة",
    "الأبيض سيدي الشيخ",
    "القنطرة",
    "بئر العاتر",
    "قصر البخاري",
    "العريشة",
)


def test_dataset_covers_all_algerian_wilayas_and_communes() -> None:
    assert len(WILAYAS) == 69
    assert sum(len(communes) for communes in WILAYAS.values()) == 1541


def test_core_wilayas_are_present() -> None:
    for wilaya in ("الجزائر", "وهران", "قسنطينة", "أدرار", "تمنراست", "تلمسان", "سطيف"):
        assert wilaya in WILAYAS


def test_new_2025_wilayas_are_present() -> None:
    for wilaya in NEW_2025_WILAYAS:
        assert wilaya in WILAYAS


def test_every_wilaya_has_communes() -> None:
    for wilaya, communes in WILAYAS.items():
        assert len(communes) >= 2, wilaya
        assert all(isinstance(name, str) and name.strip() for name in communes), wilaya
        assert len(communes) == len(set(communes)), f"duplicate commune in {wilaya}"


def test_ride_events_use_algerian_wilayas_and_communes() -> None:
    for event in generate_ride_events(300, seed=11):
        payload = event["payload"]
        wilaya = payload["city"]

        assert wilaya in WILAYAS
        if event["event_type"] == "ride_completed":
            assert payload["pickup_area"] in WILAYAS[wilaya]
            assert payload["dropoff_area"] in WILAYAS[wilaya]
            assert payload["pickup_area"] != payload["dropoff_area"]
            assert payload["currency"] == "DZD"
        elif event["event_type"] == "ride_requested":
            assert payload["pickup_area"] in WILAYAS[wilaya]


def test_order_events_use_algerian_wilayas_and_communes() -> None:
    for event in generate_order_events(300, seed=11):
        if event["event_type"] != "order_placed":
            continue

        payload = event["payload"]
        wilaya = payload["city"]

        assert wilaya in WILAYAS
        assert payload["district"] in WILAYAS[wilaya]
        assert payload["currency"] == "DZD"


@pytest.mark.parametrize("wilaya", ["الجزائر", "وهران", "بوسعادة"])
def test_wilaya_commune_lists_are_arabic(wilaya: str) -> None:
    assert all(any("\u0600" <= char <= "\u06ff" for char in name) for name in WILAYAS[wilaya])
