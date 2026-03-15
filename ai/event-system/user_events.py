"""
User domain events.

These events are produced by the service that owns the users/permissions
database, and consumed by any service that caches user state (e.g. the
UI backend holding websocket ConnectionClient objects).
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from events_lib.models.base import EventMeta, EventType


class UserUpdatedPayload(BaseModel):
    """A user's profile information was changed."""

    payload_type: Literal[EventType.USER_UPDATED] = EventType.USER_UPDATED

    user_id: str
    changed_fields: list[str] = Field(
        description="List of field names that changed, e.g. ['email', 'display_name']. "
        "Consumers use this to decide whether to invalidate their cache."
    )

    def partition_key(self) -> str:
        return self.user_id


class PermissionsUpdatedPayload(BaseModel):
    """
    A user's permissions or role assignments changed.

    scope identifies *what* changed — it could be a role name,
    a permission group, or a resource path. This lets consumers
    decide whether they need to re-evaluate access for their
    cached sessions.
    """

    payload_type: Literal[EventType.PERMISSIONS_UPDATED] = (
        EventType.PERMISSIONS_UPDATED
    )

    user_id: str
    scope: str = Field(
        description="The permission scope that changed, e.g. 'role:admin', "
        "'resource:portfolio/123', 'group:risk-traders'"
    )
    action: Literal["granted", "revoked", "modified"] = "modified"

    def partition_key(self) -> str:
        return self.user_id


# ──────────────────────────────────────────────────────────────────────
# Convenience constructors — these build the full EventEnvelope so
# producers don't need to assemble meta + payload manually every time.
# ──────────────────────────────────────────────────────────────────────


def UserUpdatedEvent(
    *,
    source_service: str,
    user_id: str,
    changed_fields: list[str],
    correlation_id: str | None = None,
) -> "EventEnvelope":
    """Build a complete UserUpdated event envelope."""
    from events_lib.models.base import EventEnvelope

    return EventEnvelope(
        meta=EventMeta(
            event_type=EventType.USER_UPDATED,
            source_service=source_service,
            correlation_id=correlation_id,
        ),
        payload=UserUpdatedPayload(
            user_id=user_id,
            changed_fields=changed_fields,
        ),
    )


def PermissionsUpdatedEvent(
    *,
    source_service: str,
    user_id: str,
    scope: str,
    action: Literal["granted", "revoked", "modified"] = "modified",
    correlation_id: str | None = None,
) -> "EventEnvelope":
    """Build a complete PermissionsUpdated event envelope."""
    from events_lib.models.base import EventEnvelope

    return EventEnvelope(
        meta=EventMeta(
            event_type=EventType.PERMISSIONS_UPDATED,
            source_service=source_service,
            correlation_id=correlation_id,
        ),
        payload=PermissionsUpdatedPayload(
            user_id=user_id,
            scope=scope,
            action=action,
        ),
    )
