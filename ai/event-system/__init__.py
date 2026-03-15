"""
events-lib: Shared event system for financial microservices.

Provides Pydantic-based event models, a unified EventsClient for
producing/consuming Kafka events, and all serialization/routing logic.
"""

from events_lib.models.base import EventEnvelope, EventMeta
from events_lib.models.user_events import UserUpdatedEvent, PermissionsUpdatedEvent
from events_lib.models.data_events import (
    DataChangeEvent,
    DataChangeType,
    DataGroupType,
)
from events_lib.models.simulation_events import (
    SimulationProblemEvent,
    SimulationProblemType,
)
from events_lib.models.system_events import (
    ServiceStatusEvent,
    ServiceHealthStatus,
    ServiceIssueEvent,
    IssueSeverity,
)
from events_lib.client import EventsClient
from events_lib.config import KafkaConfig

__all__ = [
    # Client
    "EventsClient",
    "KafkaConfig",
    # Base
    "EventEnvelope",
    "EventMeta",
    # User events
    "UserUpdatedEvent",
    "PermissionsUpdatedEvent",
    # Data events
    "DataChangeEvent",
    "DataChangeType",
    "DataGroupType",
    # Simulation events
    "SimulationProblemEvent",
    "SimulationProblemType",
    # System events
    "ServiceStatusEvent",
    "ServiceHealthStatus",
    "ServiceIssueEvent",
    "IssueSeverity",
]
