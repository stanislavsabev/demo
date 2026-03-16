"""
Usage examples for the async EventsClient.

All handlers are async — they can freely await websocket sends,
Redis operations, HTTP calls, etc.
"""

# =====================================================================
# EXAMPLE 1: UI Backend — async handlers for websocket push
# =====================================================================

from events_lib import (
    EventsClient,
    EventEnvelope,
    EventType,
    KafkaConfig,
    SimulationProblemEvent,
    SimulationProblemType,
    ServiceStatusEvent,
    ServiceHealthStatus,
    DataChangeEvent,
    DataChangeType,
    DataGroupType,
    UserUpdatedEvent,
    PermissionsUpdatedEvent,
)
from events_lib.models.user_events import UserUpdatedPayload, PermissionsUpdatedPayload
from events_lib.models.simulation_events import SimulationProblemPayload
from events_lib.models.data_events import DataChangePayload


class ConnectionManager:
    """Simplified — manages WS connections per user."""

    async def invalidate_user_cache(self, user_id: str, fields: list[str]) -> None:
        # await redis.delete(f"user:{user_id}")
        # await self._ws_connections[user_id].send_json({...})
        ...

    async def invalidate_permissions(self, user_id: str, scope: str) -> None: ...

    async def notify_simulation_problem(
        self, simulation_name: str, message: str
    ) -> None:
        # Push to all connected users watching this simulation
        # await self._broadcast_to_watchers(simulation_name, {...})
        ...


def build_ui_backend_handlers(conn_manager: ConnectionManager):
    """
    Factory that returns async handlers closed over the ConnectionManager.

    This pattern keeps handler functions testable — inject a mock
    ConnectionManager in tests.
    """

    async def handle_user_updated(envelope: EventEnvelope) -> None:
        payload: UserUpdatedPayload = envelope.payload  # type: ignore[assignment]
        await conn_manager.invalidate_user_cache(
            user_id=payload.user_id,
            fields=payload.changed_fields,
        )

    async def handle_permissions_updated(envelope: EventEnvelope) -> None:
        payload: PermissionsUpdatedPayload = envelope.payload  # type: ignore[assignment]
        await conn_manager.invalidate_permissions(
            user_id=payload.user_id,
            scope=payload.scope,
        )

    async def handle_simulation_problem(envelope: EventEnvelope) -> None:
        payload: SimulationProblemPayload = envelope.payload  # type: ignore[assignment]
        await conn_manager.notify_simulation_problem(
            simulation_name=payload.simulation_name,
            message=f"[{payload.problem_type}] {payload.message}",
        )

    return {
        EventType.USER_UPDATED: handle_user_updated,
        EventType.PERMISSIONS_UPDATED: handle_permissions_updated,
        EventType.SIMULATION_PROBLEM: handle_simulation_problem,
    }


# =====================================================================
# EXAMPLE 2: FastAPI lifespan integration
# =====================================================================

from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
# from fastapi import FastAPI  # uncomment in real code


def fastapi_lifespan_example():
    """
    Recommended pattern for wiring EventsClient into FastAPI.

    The client starts on app startup and stops on app shutdown.
    Both produce() and all handlers are async — no thread bridging.

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        conn_manager = ConnectionManager()

        client = EventsClient(
            config=KafkaConfig(),
            service_name="ui-backend",
            consumer_group="ui-backend-group",
        )

        # Register async handlers
        handlers = build_ui_backend_handlers(conn_manager)
        for event_type, handler in handlers.items():
            client.on(event_type, handler)

        # Start consuming — poll thread + dispatch task start here
        await client.start()

        # Make client available via app.state for producing from routes
        app.state.events_client = client

        yield

        # Graceful shutdown — drains queue, commits offsets, leaves group
        await client.stop()

    app = FastAPI(lifespan=lifespan)


    # Producing from a route handler — fully async, no blocking:

    @app.post("/api/admin/update-user/{user_id}")
    async def update_user(user_id: str, request: Request):
        # ... update database ...

        client: EventsClient = request.app.state.events_client
        event = UserUpdatedEvent(
            source_service="ui-backend",
            user_id=user_id,
            changed_fields=["email"],
        )
        await client.produce(event)  # Non-blocking

        return {"status": "updated"}
    """
    pass


# =====================================================================
# EXAMPLE 3: Orchestrator — both produces and consumes
# =====================================================================

class SimulationRegistry:
    """Simplified — tracks active simulations."""

    async def notify_data_change(self, data_group: str, data_key: str) -> None:
        # Propagate to affected simulations and calculators
        ...


async def orchestrator_example() -> None:
    """
    The Orchestrator is both consumer (data changes) and producer
    (simulation problems, status heartbeats).

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        sim_registry = SimulationRegistry()

        client = EventsClient(
            config=KafkaConfig(),
            service_name="orchestrator",
            consumer_group="orchestrator-group",
        )

        async def handle_data_change(envelope: EventEnvelope) -> None:
            payload: DataChangePayload = envelope.payload
            await sim_registry.notify_data_change(
                data_group=payload.data_group,
                data_key=payload.data_key,
            )

        client.on(EventType.DATA_CHANGE, handle_data_change)
        await client.start()
        app.state.events_client = client

        yield

        await client.stop()

    # Later, when a simulation fails — produce from a route or internal logic:

    async def report_simulation_failure(client: EventsClient) -> None:
        event = SimulationProblemEvent(
            source_service="orchestrator",
            simulation_name="EOD-VaR-2024Q1",
            problem_type=SimulationProblemType.CALCULATOR_FAILURE,
            message="Calculator calc-risk-007 crashed during VaR aggregation",
            calculator_id="calc-risk-007",
            context={
                "portfolio": "EMEA-FX-OPTIONS",
                "last_trade_processed": "TRD-2024-88421",
            },
            is_recoverable=False,
        )
        await client.produce(event)


    # Periodic heartbeat — run as a background task:

    async def heartbeat_loop(client: EventsClient) -> None:
        import asyncio
        while True:
            status = ServiceStatusEvent(
                source_service="orchestrator",
                service_name="orchestrator",
                instance_id="orch-pod-3a",
                status=ServiceHealthStatus.HEALTHY,
                metrics={
                    "active_simulations": 12,
                    "active_calculators": 48,
                    "avg_calc_time_ms": 23.5,
                },
                uptime_seconds=86400.0,
            )
            await client.produce(status)
            await asyncio.sleep(30)  # Every 30 seconds
    """
    pass
