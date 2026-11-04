"""Event type vocabulary shared by producers, the pipeline, and the agent.

The catalogue and topic mapping are part of the data contract
(``docs/data-contracts.md``, section 1.3).
"""

from __future__ import annotations

from enum import StrEnum


class EventType(StrEnum):
    RIDE_REQUESTED = "ride_requested"
    RIDE_COMPLETED = "ride_completed"
    RIDE_CANCELLED = "ride_cancelled"
    ORDER_PLACED = "order_placed"
    ORDER_CANCELLED = "order_cancelled"
    PAYMENT_CAPTURED = "payment_captured"
    PAYMENT_FAILED = "payment_failed"


TOPIC_BY_EVENT_TYPE: dict[EventType, str] = {
    EventType.RIDE_REQUESTED: "ride_events",
    EventType.RIDE_COMPLETED: "ride_events",
    EventType.RIDE_CANCELLED: "ride_events",
    EventType.ORDER_PLACED: "order_events",
    EventType.ORDER_CANCELLED: "order_events",
    EventType.PAYMENT_CAPTURED: "payment_events",
    EventType.PAYMENT_FAILED: "payment_events",
}

RIDE_EVENT_TYPES = (
    EventType.RIDE_REQUESTED.value,
    EventType.RIDE_COMPLETED.value,
    EventType.RIDE_CANCELLED.value,
)

ORDER_EVENT_TYPES = (
    EventType.ORDER_PLACED.value,
    EventType.ORDER_CANCELLED.value,
)

PAYMENT_EVENT_TYPES = (
    EventType.PAYMENT_CAPTURED.value,
    EventType.PAYMENT_FAILED.value,
)
