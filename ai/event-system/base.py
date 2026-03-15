"""
Base event models: envelope, metadata, and the discriminated union registry.

Every event flows through Kafka wrapped in an EventEnvelope:
    {
        "meta": { "event_id": "...", "event_type": "user.updated", ... },
        "payload": { ... }   // The actual event data, type-specific
    }

The envelope carries routing metadata (event_type, source_service, timestamps,
correlation/causation IDs for tracing). The payload is a discriminated union
of all known event types, resolved via the `event_type` field in meta.

This design means:
  - Serialization/deserialization is handled once, in the shared lib.
  - Any service can deserialize any event without knowing the producer.
  - New event types are added by defining a model and registering it.
  - mypy and Pydantic both validate the full event structure.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated, Union

from pydantic import BaseModel, Field


class EventType(StrEnum):
    """
    Registry of all event types in the system.

    Convention: <domain>.<action> — the domain maps to a Kafka topic,
    the action distinguishes events within that topic.
    """

    # User domain -> topic: user-events
    USER_UPDATED = "user.updated"
    PERMISSIONS_UPDATED = "user.permissions_updated"

    # Data domain -> topic: data-changes
    DATA_CHANGE = "data.change"

    # Simulation domain -> topic: simulation-events
    SIMULATION_PROBLEM = "simulation.problem"

    # System domain -> topic: system-status
    SERVICE_STATUS = "system.service_status"
    SERVICE_ISSUE = "system.service_issue"


# Maps event types to their Kafka topic.
# This is the single source of truth for routing.
TOPIC_ROUTING: dict[EventType, str] = {
    EventType.USER_UPDATED: "user-events",
    EventType.PERMISSIONS_UPDATED: "user-events",
    EventType.DATA_CHANGE: "data-changes",
    EventType.SIMULATION_PROBLEM: "simulation-events",
    EventType.SERVICE_STATUS: "system-status",
    EventType.SERVICE_ISSUE: "system-status",
}


class EventMeta(BaseModel):
    """
    Metadata envelope for every event.

    Fields follow CloudEvents-inspired conventions:
    - event_id: Globally unique, used for idempotency checks by consumers.
    - event_type: Discriminator for payload deserialization.
    - source_service: Which service produced this event.
    - timestamp: When the event was created (UTC).
    - correlation_id: Ties related events across a workflow (e.g. a user
      action that triggers multiple downstream events).
    - causation_id: The event_id of the event that directly caused this one.
      Enables building causal chains for debugging.
    - schema_version: Payload schema version for forward/backward compat.
    """

    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: EventType
    source_service: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    correlation_id: str | None = None
    causation_id: str | None = None
    schema_version: int = 1


# ──────────────────────────────────────────────────────────────────────
# Forward references — the actual payload types are defined in their
# respective modules. We import them lazily and build the union after
# all modules are loaded. See EventEnvelope.model_rebuild() at bottom.
# ──────────────────────────────────────────────────────────────────────


def _build_payload_union() -> type:
    """
    Build the discriminated union of all event payload types.

    Called once at import time after all event modules are loaded.
    This avoids circular imports: each event module imports EventMeta
    from here, but doesn't need the union type.
    """
    from events_lib.models.data_events import DataChangePayload
    from events_lib.models.simulation_events import SimulationProblemPayload
    from events_lib.models.system_events import (
        ServiceIssuePayload,
        ServiceStatusPayload,
    )
    from events_lib.models.user_events import (
        PermissionsUpdatedPayload,
        UserUpdatedPayload,
    )

    return Annotated[
        Union[
            UserUpdatedPayload,
            PermissionsUpdatedPayload,
            DataChangePayload,
            SimulationProblemPayload,
            ServiceStatusPayload,
            ServiceIssuePayload,
        ],
        Field(discriminator="payload_type"),
    ]


class EventEnvelope(BaseModel):
    """
    Top-level event structure serialized to/from Kafka.

    The payload field is a discriminated union — Pydantic resolves the
    correct type based on `payload.payload_type` matching `meta.event_type`.
    """

    meta: EventMeta
    payload: object  # Replaced by model_rebuild() below

    def kafka_key(self) -> str:
        """
        Derive the Kafka message key from the payload.

        The key determines partition assignment. All events for the same
        entity (user_id, simulation_name, service_name) land in the same
        partition, preserving ordering per entity.
        """
        return self.payload.partition_key()  # type: ignore[union-attr]

    def topic(self) -> str:
        """Resolve the Kafka topic from the event type."""
        return TOPIC_ROUTING[self.meta.event_type]


def rebuild_envelope() -> None:
    """Rebuild EventEnvelope with the full payload union. Call once at import."""
    PayloadUnion = _build_payload_union()
    EventEnvelope.model_fields["payload"] = Field(discriminator="payload_type")
    EventEnvelope.__annotations__["payload"] = PayloadUnion
    EventEnvelope.model_rebuild(force=True)
