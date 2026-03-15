"""
Usage examples for each service in the financial system.

These are NOT runnable as-is — they show how each service integrates
with events-lib. Copy the patterns relevant to your service.
"""

# =====================================================================
# EXAMPLE 1: Data Service (Producer)
#
# This service owns the database. When data changes, it produces events.
# Uses the outbox pattern for consistency (see note below).
# =====================================================================

from events_lib import (
    DataChangeEvent,
    DataChangeType,
    DataGroupType,
    EventsClient,
    KafkaConfig,
    UserUpdatedEvent,
    PermissionsUpdatedEvent,
)


def data_service_example() -> None:
    # Producer-only — no consumer_group needed
    client = EventsClient(
        config=KafkaConfig(),
        service_name="data-service",
    )

    # After writing a fixing to the DB, emit the event:
    event = DataChangeEvent(
        source_service="data-service",
        data_group=DataGroupType.STATIC,
        data_key="fixing:USD/EUR/2024-03-15",
        change_type=DataChangeType.UPDATED,
        changed_by="admin@firm.com",
        details={"old_rate": 1.0812, "new_rate": 1.0825},
    )
    client.produce(event)

    # User profile update
    event = UserUpdatedEvent(
        source_service="data-service",
        user_id="usr-456",
        changed_fields=["email", "display_name"],
    )
    client.produce(event)

    # Permission change
    event = PermissionsUpdatedEvent(
        source_service="data-service",
        user_id="usr-456",
        scope="role:risk-viewer",
        action="granted",
    )
    client.produce(event)

    client.flush()


# =====================================================================
# EXAMPLE 2: UI Backend (Consumer)
#
# Holds websocket connections with cached user info. Needs to react
# to user/permission changes and simulation problems.
# =====================================================================

from events_lib import (
    EventEnvelope,
    EventType,
)
from events_lib.models.base import EventMeta
from events_lib.models.user_events import UserUpdatedPayload, PermissionsUpdatedPayload
from events_lib.models.simulation_events import SimulationProblemPayload


class ConnectionManager:
    """Simplified — manages WS connections per user."""

    def invalidate_user_cache(self, user_id: str, fields: list[str]) -> None: ...
    def invalidate_permissions(self, user_id: str, scope: str) -> None: ...
    def notify_simulation_problem(
        self, simulation_name: str, message: str
    ) -> None: ...


def ui_backend_example() -> None:
    conn_manager = ConnectionManager()

    def handle_user_updated(envelope: EventEnvelope) -> None:
        payload: UserUpdatedPayload = envelope.payload  # type: ignore[assignment]
        conn_manager.invalidate_user_cache(
            user_id=payload.user_id,
            fields=payload.changed_fields,
        )

    def handle_permissions_updated(envelope: EventEnvelope) -> None:
        payload: PermissionsUpdatedPayload = envelope.payload  # type: ignore[assignment]
        conn_manager.invalidate_permissions(
            user_id=payload.user_id,
            scope=payload.scope,
        )

    def handle_simulation_problem(envelope: EventEnvelope) -> None:
        payload: SimulationProblemPayload = envelope.payload  # type: ignore[assignment]
        conn_manager.notify_simulation_problem(
            simulation_name=payload.simulation_name,
            message=f"[{payload.problem_type}] {payload.message}",
        )

    client = EventsClient(
        config=KafkaConfig(),
        service_name="ui-backend",
        consumer_group="ui-backend-group",
    )

    client.on(EventType.USER_UPDATED, handle_user_updated)
    client.on(EventType.PERMISSIONS_UPDATED, handle_permissions_updated)
    client.on(EventType.SIMULATION_PROBLEM, handle_simulation_problem)

    client.start()
    # ... FastAPI lifespan keeps this alive ...
    # client.stop() on shutdown


# =====================================================================
# EXAMPLE 3: Orchestrator (Producer + Consumer)
#
# Consumes data-change events to propagate to simulations.
# Produces simulation-problem events when things go wrong.
# Also produces heartbeat status events.
# =====================================================================

from events_lib import (
    SimulationProblemEvent,
    SimulationProblemType,
    ServiceStatusEvent,
    ServiceHealthStatus,
)
from events_lib.models.data_events import DataChangePayload


class SimulationRegistry:
    """Simplified — tracks active simulations."""

    def notify_data_change(
        self, data_group: str, data_key: str
    ) -> None: ...


def orchestrator_example() -> None:
    sim_registry = SimulationRegistry()

    # This client is both producer AND consumer
    client = EventsClient(
        config=KafkaConfig(),
        service_name="orchestrator",
        consumer_group="orchestrator-group",
    )

    def handle_data_change(envelope: EventEnvelope) -> None:
        payload: DataChangePayload = envelope.payload  # type: ignore[assignment]
        sim_registry.notify_data_change(
            data_group=payload.data_group,
            data_key=payload.data_key,
        )

    client.on(EventType.DATA_CHANGE, handle_data_change)
    client.start()

    # ... later, when a simulation fails:
    event = SimulationProblemEvent(
        source_service="orchestrator",
        simulation_name="EOD-VaR-2024Q1",
        problem_type=SimulationProblemType.CALCULATOR_FAILURE,
        message="Calculator calc-risk-007 crashed during VaR aggregation",
        calculator_id="calc-risk-007",
        context={
            "portfolio": "EMEA-FX-OPTIONS",
            "last_trade_processed": "TRD-2024-88421",
            "error": "ZeroDivisionError in risk_factor_sensitivity()",
        },
        is_recoverable=False,
    )
    client.produce(event)

    # Periodic heartbeat (call from a background task)
    status = ServiceStatusEvent(
        source_service="orchestrator",
        service_name="orchestrator",
        instance_id="orch-pod-3a",
        status=ServiceHealthStatus.HEALTHY,
        metrics={
            "active_simulations": 12,
            "active_calculators": 48,
            "avg_calc_time_ms": 23.5,
            "pending_data_events": 3,
        },
        uptime_seconds=86400.0,
    )
    client.produce(status)


# =====================================================================
# EXAMPLE 4: FastAPI integration via lifespan
#
# Shows how to wire EventsClient into FastAPI's startup/shutdown cycle.
# =====================================================================

from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
# from fastapi import FastAPI  # uncomment in real code


def fastapi_integration_example():
    """
    Demonstrates the recommended FastAPI integration pattern.

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        # Startup
        events_client = EventsClient(
            config=KafkaConfig(),
            service_name="ui-backend",
            consumer_group="ui-backend-group",
        )

        # Register handlers
        events_client.on(EventType.USER_UPDATED, handle_user_updated)
        events_client.on(EventType.SIMULATION_PROBLEM, handle_sim_problem)

        # Start consuming
        events_client.start()

        # Make client available via app.state
        app.state.events_client = events_client

        yield

        # Shutdown
        events_client.stop()

    app = FastAPI(lifespan=lifespan)

    @app.post("/api/admin/force-recalc")
    async def force_recalc(simulation_name: str):
        # Access the client from app.state to produce events
        client: EventsClient = app.state.events_client
        # ... produce event ...
    """
    pass
