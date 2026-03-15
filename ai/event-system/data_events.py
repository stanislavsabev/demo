"""
Data-change domain events.

Produced by the service that manages the underlying data (statics,
configurations, mappings). Consumed by the Orchestrator, Simulations,
and Calculators that depend on this data.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, Field

from events_lib.models.base import EventMeta, EventType


class DataGroupType(StrEnum):
    """The broad category of data that changed."""

    STATIC = "static"  # assignments, fixings, etc.
    CONFIGURATION = "configuration"
    MAPPING = "mapping"


class DataChangeType(StrEnum):
    """What happened to the data."""

    CREATED = "created"
    UPDATED = "updated"
    DELETED = "deleted"
    BULK_RELOAD = "bulk_reload"  # Full refresh of a data group


class DataChangePayload(BaseModel):
    """
    A specific data record (or group) was modified.

    The combination of data_group + data_key uniquely identifies
    the affected data. For example:
      - data_group=STATIC, data_key="fixing:USD/EUR/2024-03-15"
      - data_group=CONFIGURATION, data_key="risk-model:var-99"
      - data_group=MAPPING, data_key="portfolio-to-book:portfolio-123"

    For BULK_RELOAD, data_key may be "*" to indicate the entire group
    was refreshed.
    """

    payload_type: Literal[EventType.DATA_CHANGE] = EventType.DATA_CHANGE

    data_group: DataGroupType
    data_key: str = Field(
        description="Specific identifier within the data group. "
        "Convention: <entity_type>:<entity_id>"
    )
    change_type: DataChangeType
    changed_by: str | None = Field(
        default=None,
        description="User or system that triggered the change, for audit trail.",
    )
    details: dict[str, object] | None = Field(
        default=None,
        description="Optional extra context — e.g. which fields changed, "
        "old vs new values for critical fields.",
    )

    def partition_key(self) -> str:
        """
        Key by data_group + data_key.

        This ensures all changes to the same data record are ordered.
        Consumers processing 'fixing:USD/EUR/2024-03-15' see creates
        before updates before deletes, in order.
        """
        return f"{self.data_group}:{self.data_key}"


def DataChangeEvent(
    *,
    source_service: str,
    data_group: DataGroupType,
    data_key: str,
    change_type: DataChangeType,
    changed_by: str | None = None,
    details: dict[str, object] | None = None,
    correlation_id: str | None = None,
) -> "EventEnvelope":
    """Build a complete DataChange event envelope."""
    from events_lib.models.base import EventEnvelope

    return EventEnvelope(
        meta=EventMeta(
            event_type=EventType.DATA_CHANGE,
            source_service=source_service,
            correlation_id=correlation_id,
        ),
        payload=DataChangePayload(
            data_group=data_group,
            data_key=data_key,
            change_type=change_type,
            changed_by=changed_by,
            details=details,
        ),
    )
