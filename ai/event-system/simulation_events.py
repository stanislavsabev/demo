"""
Simulation domain events.

Produced by the Orchestrator when a Simulation encounters problems.
Consumed by the UI backend to surface issues to users in real time.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, Field

from events_lib.models.base import EventMeta, EventType


class SimulationProblemType(StrEnum):
    """Categories of simulation failures."""

    CALCULATOR_FAILURE = "calculator_failure"
    DATA_MISSING = "data_missing"
    TIMEOUT = "timeout"
    CONFIGURATION_ERROR = "configuration_error"
    RESOURCE_EXHAUSTION = "resource_exhaustion"
    CONVERGENCE_FAILURE = "convergence_failure"


class SimulationProblemPayload(BaseModel):
    """
    A simulation experienced a problem that needs attention.

    simulation_name is the human-readable identifier shown in the UI.
    calculator_id is optional — only set when the problem is traceable
    to a specific Calculator instance.
    """

    payload_type: Literal[EventType.SIMULATION_PROBLEM] = (
        EventType.SIMULATION_PROBLEM
    )

    simulation_name: str
    problem_type: SimulationProblemType
    calculator_id: str | None = None
    message: str = Field(
        description="Human-readable description of what went wrong."
    )
    context: dict[str, object] | None = Field(
        default=None,
        description="Structured debugging context — e.g. which trade failed, "
        "which risk factor was missing, stack trace excerpt.",
    )
    is_recoverable: bool = Field(
        default=True,
        description="Whether the simulation can continue after this problem. "
        "False means the simulation is dead and needs manual restart.",
    )

    def partition_key(self) -> str:
        return self.simulation_name


def SimulationProblemEvent(
    *,
    source_service: str,
    simulation_name: str,
    problem_type: SimulationProblemType,
    message: str,
    calculator_id: str | None = None,
    context: dict[str, object] | None = None,
    is_recoverable: bool = True,
    correlation_id: str | None = None,
) -> "EventEnvelope":
    """Build a complete SimulationProblem event envelope."""
    from events_lib.models.base import EventEnvelope

    return EventEnvelope(
        meta=EventMeta(
            event_type=EventType.SIMULATION_PROBLEM,
            source_service=source_service,
            correlation_id=correlation_id,
        ),
        payload=SimulationProblemPayload(
            simulation_name=simulation_name,
            problem_type=problem_type,
            message=message,
            calculator_id=calculator_id,
            context=context,
            is_recoverable=is_recoverable,
        ),
    )
