"""
System observability domain events.

Every service periodically emits ServiceStatusEvent (heartbeat with
metrics). When something goes wrong, services emit ServiceIssueEvent.

The System Observer service consumes these to build dashboards,
trigger email alerts, and maintain the overall system health view.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, Field

from events_lib.models.base import EventMeta, EventType


class ServiceHealthStatus(StrEnum):
    """Coarse health indicator for a service."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    STARTING = "starting"
    SHUTTING_DOWN = "shutting_down"


class IssueSeverity(StrEnum):
    """Severity levels for service issues."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class ServiceStatusPayload(BaseModel):
    """
    Periodic heartbeat / status report from a service.

    Services emit this every N seconds (configurable). The System Observer
    uses absence of heartbeats as a signal that a service is down.

    metrics is intentionally a flat dict — each service reports whatever
    is meaningful for it (queue depth, active connections, memory usage,
    calculation throughput, etc.). The Observer can aggregate and alert
    on any numeric metric.
    """

    payload_type: Literal[EventType.SERVICE_STATUS] = EventType.SERVICE_STATUS

    service_name: str
    instance_id: str = Field(
        description="Distinguishes multiple instances of the same service. "
        "Typically hostname or pod name."
    )
    status: ServiceHealthStatus
    metrics: dict[str, float] = Field(
        default_factory=dict,
        description="Arbitrary numeric metrics. Examples: "
        "{'active_ws_connections': 142, 'memory_mb': 512.3, "
        "'avg_calc_time_ms': 23.5}",
    )
    uptime_seconds: float = 0.0

    def partition_key(self) -> str:
        return self.service_name


class ServiceIssuePayload(BaseModel):
    """
    A specific issue that a service wants to flag.

    Unlike the heartbeat, this is event-driven — emitted when something
    noteworthy happens (not periodically). The Observer can correlate
    issues across services using correlation_id in the envelope.
    """

    payload_type: Literal[EventType.SERVICE_ISSUE] = EventType.SERVICE_ISSUE

    service_name: str
    instance_id: str
    severity: IssueSeverity
    category: str = Field(
        description="Machine-readable issue category, e.g. 'kafka_lag', "
        "'db_connection_pool_exhausted', 'memory_pressure', 'slow_query'."
    )
    message: str
    details: dict[str, object] | None = None

    def partition_key(self) -> str:
        return self.service_name


def ServiceStatusEvent(
    *,
    source_service: str,
    service_name: str,
    instance_id: str,
    status: ServiceHealthStatus,
    metrics: dict[str, float] | None = None,
    uptime_seconds: float = 0.0,
) -> "EventEnvelope":
    from events_lib.models.base import EventEnvelope

    return EventEnvelope(
        meta=EventMeta(
            event_type=EventType.SERVICE_STATUS,
            source_service=source_service,
        ),
        payload=ServiceStatusPayload(
            service_name=service_name,
            instance_id=instance_id,
            status=status,
            metrics=metrics or {},
            uptime_seconds=uptime_seconds,
        ),
    )


def ServiceIssueEvent(
    *,
    source_service: str,
    service_name: str,
    instance_id: str,
    severity: IssueSeverity,
    category: str,
    message: str,
    details: dict[str, object] | None = None,
    correlation_id: str | None = None,
) -> "EventEnvelope":
    from events_lib.models.base import EventEnvelope

    return EventEnvelope(
        meta=EventMeta(
            event_type=EventType.SERVICE_ISSUE,
            source_service=source_service,
            correlation_id=correlation_id,
        ),
        payload=ServiceIssuePayload(
            service_name=service_name,
            instance_id=instance_id,
            severity=severity,
            category=category,
            message=message,
            details=details,
        ),
    )
