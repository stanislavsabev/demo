"""
EventsClient: Unified async producer/consumer for the event system.

Architecture:
    ┌─────────────────────────┐      ┌─────────────────────────────┐
    │  Thread: _poll_loop()   │      │  asyncio.Task: _dispatch()  │
    │                         │      │                             │
    │  confluent_kafka.poll() │─────>│  asyncio.Queue              │
    │  deserialize + dedup    │ put  │  retry + DLQ logic          │
    │  commit offsets         │      │  await handler(envelope)    │
    └─────────────────────────┘      └─────────────────────────────┘

    The poll thread does only: poll, deserialize, dedup check, enqueue.
    ALL handler dispatch happens in the async task — handlers can freely
    await websocket sends, Redis calls, HTTP requests, etc.

    Offset commits happen after the async handler completes, via a
    callback from the dispatch task back to the poll thread. This ensures
    at-least-once semantics: we don't commit until the handler succeeds.

Design decisions:
    - Programmatic handler registration (not decorators).
    - Handlers are registered per EventType, not per topic.
    - Fully async handlers — no thread bridging needed by the caller.
    - confluent_kafka for Kafka protocol (production-grade, C-backed).
    - asyncio.Queue for thread→async handoff (bounded, backpressure).
    - DLQ support for deserialization failures and handler exhaustion.
    - Graceful shutdown with proper drain and consumer group leave.

Usage:
    client = EventsClient(
        config=KafkaConfig(),
        service_name="ui-backend",
        consumer_group="ui-backend-group",
    )

    # Register async handlers
    client.on(EventType.USER_UPDATED, handle_user_update)
    client.on(EventType.SIMULATION_PROBLEM, handle_sim_problem)

    # Start (in FastAPI lifespan)
    await client.start()

    # Produce (from any coroutine)
    await client.produce(event)

    # Shutdown
    await client.stop()
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

from events_lib.config import KafkaConfig
from events_lib.models.base import (
    TOPIC_ROUTING,
    EventEnvelope,
    EventType,
    rebuild_envelope,
)

if TYPE_CHECKING:
    from confluent_kafka import Message

logger = logging.getLogger("events_lib")

# Ensure the discriminated union is built before any deserialization
rebuild_envelope()

# Handler type — always async
AsyncEventHandler = Callable[[EventEnvelope], Awaitable[None]]


@dataclass(slots=True)
class _QueueItem:
    """
    Internal message passed from poll thread to dispatch task.

    Carries the deserialized envelope plus the raw Kafka message
    (needed for offset commits and DLQ forwarding).
    """

    envelope: EventEnvelope
    raw_msg: Message


@dataclass(slots=True)
class _CommitRequest:
    """Sent back from the dispatch task to the poll thread for offset commit."""

    msg: Message


class EventsClient:
    """
    Unified async Kafka producer/consumer.

    The poll thread and dispatch task are fully internal. From the
    caller's perspective, this is a pure async interface.
    """

    def __init__(
        self,
        config: KafkaConfig,
        service_name: str,
        consumer_group: str | None = None,
        *,
        max_retries: int | None = None,
        idempotency_window: int = 10_000,
        queue_max_size: int = 1_000,
    ) -> None:
        """
        Args:
            config: Kafka connection configuration.
            service_name: Name of this service (used in event metadata).
            consumer_group: Kafka consumer group ID. None = producer-only mode.
            max_retries: Override config's max_retries_before_dlq.
            idempotency_window: Max event IDs to track for dedup.
            queue_max_size: Bounded queue size. When full, the poll thread
                blocks — this is intentional backpressure. If handlers are
                slower than the poll rate, the queue fills up, the poll
                thread stalls, and Kafka rebalancing eventually kicks in.
                Size this based on your handler latency × poll throughput.
        """
        self._config = config
        self._service_name = service_name
        self._consumer_group = consumer_group
        self._max_retries = max_retries or config.max_retries_before_dlq

        # Handler registry: EventType -> list of async handler callables
        self._handlers: dict[EventType, list[AsyncEventHandler]] = defaultdict(list)

        # Producer (always available)
        self._producer = Producer(config.to_producer_config())

        # Consumer (lazy — created on start())
        self._consumer: Consumer | None = None

        # Lifecycle
        self._running = False
        self._loop: asyncio.AbstractEventLoop | None = None
        self._dispatch_task: asyncio.Task[None] | None = None

        # Thread → async handoff queue (bounded for backpressure)
        self._queue: asyncio.Queue[_QueueItem | None] = asyncio.Queue(
            maxsize=queue_max_size
        )

        # Async → thread commit channel
        # The dispatch task puts committed messages here; the poll thread
        # picks them up on each iteration and calls consumer.commit().
        self._commit_queue: asyncio.Queue[_CommitRequest] = asyncio.Queue()

        # Idempotency tracking
        self._seen_ids: set[str] = set()
        self._seen_ids_order: list[str] = []
        self._idempotency_window = idempotency_window

    # ── Handler registration ──────────────────────────────────────────

    def on(
        self,
        event_type: EventType,
        handler: AsyncEventHandler,
    ) -> None:
        """
        Register an async handler for an event type.

        Multiple handlers per event type are supported — they execute
        sequentially (awaited in order). Register before calling start().

        The handler signature is:
            async def my_handler(envelope: EventEnvelope) -> None: ...
        """
        self._handlers[event_type].append(handler)
        logger.info(
            "Registered handler %s for %s",
            handler.__qualname__,
            event_type,
        )

    def on_many(
        self,
        event_types: list[EventType],
        handler: AsyncEventHandler,
    ) -> None:
        """Register the same async handler for multiple event types."""
        for et in event_types:
            self.on(et, handler)

    # ── Production ────────────────────────────────────────────────────

    async def produce(
        self,
        event: EventEnvelope,
    ) -> None:
        """
        Produce an event to Kafka.

        Wraps the synchronous confluent_kafka produce in a thread executor
        to avoid blocking the event loop. The actual network send is
        buffered by librdkafka and happens asynchronously in its own
        internal threads regardless.
        """
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._produce_sync, event)

    def _produce_sync(self, event: EventEnvelope) -> None:
        """Synchronous produce — runs in thread executor."""
        topic = event.topic()
        key = event.kafka_key()
        value = event.model_dump_json()

        headers = {
            "event_type": event.meta.event_type.encode(),
            "source_service": event.meta.source_service.encode(),
            "schema_version": str(event.meta.schema_version).encode(),
        }

        def _delivery_callback(err: Exception | None, msg: Message) -> None:
            if err:
                logger.error(
                    "Failed to deliver event %s to %s: %s",
                    event.meta.event_id,
                    topic,
                    err,
                )
            else:
                logger.debug(
                    "Delivered %s to %s [partition=%s, offset=%s]",
                    event.meta.event_type,
                    msg.topic(),
                    msg.partition(),
                    msg.offset(),
                )

        self._producer.produce(
            topic=topic,
            key=key.encode(),
            value=value.encode(),
            headers=headers,
            callback=_delivery_callback,
        )
        self._producer.poll(0)

    async def flush(self, timeout: float = 10.0) -> int:
        """Wait for all buffered events to be delivered."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None, self._producer.flush, timeout
        )

    # ── Lifecycle ─────────────────────────────────────────────────────

    async def start(self) -> None:
        """
        Start the consumer pipeline.

        Creates:
          1. A confluent_kafka Consumer subscribed to relevant topics.
          2. A background thread running the poll loop (via run_in_executor).
          3. An asyncio.Task running the dispatch loop.

        Does nothing if no handlers are registered (producer-only mode).
        """
        if not self._handlers:
            logger.info("No handlers registered — running in producer-only mode.")
            return

        if not self._consumer_group:
            raise ValueError(
                "consumer_group is required when handlers are registered."
            )

        self._loop = asyncio.get_running_loop()
        topics = self._resolve_subscribed_topics()

        logger.info(
            "Subscribing to topics: %s (group: %s)",
            topics,
            self._consumer_group,
        )

        self._consumer = Consumer(
            self._config.to_consumer_config(self._consumer_group)
        )
        self._consumer.subscribe(topics)
        self._running = True

        # Start the poll thread (via executor — asyncio manages the thread)
        self._loop.run_in_executor(None, self._poll_loop)

        # Start the async dispatch task
        self._dispatch_task = asyncio.create_task(
            self._dispatch_loop(),
            name=f"events-dispatch-{self._service_name}",
        )

        logger.info("EventsClient started for %s", self._service_name)

    async def stop(self, timeout: float = 10.0) -> None:
        """
        Graceful shutdown sequence:
          1. Signal the poll thread to stop.
          2. Send sentinel (None) to unblock the dispatch task.
          3. Wait for the dispatch task to drain remaining items.
          4. Close the Kafka consumer (triggers group leave).
          5. Flush the producer.
        """
        logger.info("Stopping EventsClient for %s...", self._service_name)
        self._running = False

        # Sentinel to unblock the dispatch task if it's waiting on get()
        await self._queue.put(None)

        # Wait for dispatch to finish processing queued items
        if self._dispatch_task and not self._dispatch_task.done():
            try:
                await asyncio.wait_for(self._dispatch_task, timeout=timeout)
            except asyncio.TimeoutError:
                logger.warning(
                    "Dispatch task did not finish within timeout — cancelling"
                )
                self._dispatch_task.cancel()
                try:
                    await self._dispatch_task
                except asyncio.CancelledError:
                    pass

        # Close consumer (synchronous — run in executor)
        if self._consumer:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._consumer.close)
            self._consumer = None

        # Flush producer
        await self.flush(timeout)
        logger.info("EventsClient stopped for %s", self._service_name)

    # ── Poll thread (runs in executor) ────────────────────────────────

    def _poll_loop(self) -> None:
        """
        Runs in a background thread via run_in_executor.

        Responsibilities (and ONLY these):
          - Call consumer.poll()
          - Deserialize the message
          - Check idempotency
          - Enqueue to the asyncio.Queue
          - Process pending offset commits from the dispatch task

        Does NOT call handlers. Does NOT do retries. Those happen in
        the async dispatch task.
        """
        assert self._consumer is not None
        assert self._loop is not None

        while self._running:
            try:
                # Process any pending offset commits from the dispatch task
                self._process_pending_commits()

                msg = self._consumer.poll(timeout=1.0)
                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error("Consumer error: %s", msg.error())
                    continue

                raw_value = msg.value()
                if raw_value is None:
                    self._consumer.commit(msg)
                    continue

                # Deserialize
                try:
                    envelope = EventEnvelope.model_validate_json(raw_value)
                except Exception:
                    logger.exception(
                        "Deserialization failed for %s [%s/%s] — sending to DLQ",
                        msg.topic(),
                        msg.partition(),
                        msg.offset(),
                    )
                    self._send_to_dlq(msg, reason="deserialization_error")
                    self._consumer.commit(msg)
                    continue

                # Idempotency check (thread-safe — see note on _mark_seen)
                if envelope.meta.event_id in self._seen_ids:
                    logger.debug(
                        "Duplicate event %s — skipping",
                        envelope.meta.event_id,
                    )
                    self._consumer.commit(msg)
                    continue

                # Check if we have handlers for this event type
                if envelope.meta.event_type not in self._handlers:
                    self._consumer.commit(msg)
                    continue

                # Enqueue for async dispatch.
                # This is the blocking backpressure point: if the queue is
                # full (handlers are slow), this call blocks the poll thread,
                # which stops polling, which eventually triggers Kafka
                # rebalancing if we exceed max.poll.interval.ms.
                # This is the correct behavior — we WANT backpressure.
                item = _QueueItem(envelope=envelope, raw_msg=msg)
                asyncio.run_coroutine_threadsafe(
                    self._queue.put(item), self._loop
                ).result(timeout=30.0)

            except KafkaException as exc:
                logger.error("Kafka consumer error: %s", exc)
                time.sleep(1)
            except Exception:
                logger.exception("Unexpected error in poll loop")
                time.sleep(1)

        logger.debug("Poll loop exiting for %s", self._service_name)

    def _process_pending_commits(self) -> None:
        """
        Drain the commit queue — commit offsets for messages that the
        dispatch task has successfully processed.

        Called from the poll thread on each iteration.
        """
        assert self._consumer is not None

        while True:
            try:
                req = self._commit_queue.get_nowait()
                self._consumer.commit(req.msg)
            except asyncio.QueueEmpty:
                break
            except Exception:
                logger.exception("Failed to commit offset")

    def _send_to_dlq(self, msg: Message, reason: str) -> None:
        """Produce a failed message to the dead letter queue topic."""
        dlq_topic = msg.topic() + self._config.dlq_topic_suffix
        headers = {
            "dlq_reason": reason.encode(),
            "original_topic": msg.topic().encode(),
            "original_partition": str(msg.partition()).encode(),
            "original_offset": str(msg.offset()).encode(),
            "consumer_group": (self._consumer_group or "unknown").encode(),
        }
        try:
            self._producer.produce(
                topic=dlq_topic,
                key=msg.key(),
                value=msg.value(),
                headers=headers,
            )
            self._producer.poll(0)
        except Exception:
            logger.exception("Failed to produce to DLQ topic %s", dlq_topic)

    # ── Async dispatch task ───────────────────────────────────────────

    async def _dispatch_loop(self) -> None:
        """
        Async task that drains the queue and calls handlers.

        This runs in the event loop — handlers can freely await
        async operations (websocket sends, Redis, HTTP, etc.).

        Retry logic with exponential backoff lives here. On exhaustion,
        the message is sent to the DLQ via the poll thread's producer.
        """
        while True:
            item = await self._queue.get()

            # Sentinel: None means shutdown
            if item is None:
                # Drain remaining items before exiting
                while not self._queue.empty():
                    remaining = self._queue.get_nowait()
                    if remaining is not None:
                        await self._dispatch_single(remaining)
                break

            await self._dispatch_single(item)

    async def _dispatch_single(self, item: _QueueItem) -> None:
        """Dispatch a single event to all registered handlers, with retries."""
        envelope = item.envelope
        event_type = envelope.meta.event_type
        handlers = self._handlers.get(event_type, [])

        all_succeeded = True

        for handler in handlers:
            succeeded = await self._call_handler_with_retries(
                handler, envelope, item.raw_msg
            )
            if not succeeded:
                all_succeeded = False
                break  # Don't run subsequent handlers if one fails fatally

        # Mark as seen (only after processing, regardless of success —
        # we don't want to re-process DLQ'd events on redelivery)
        self._mark_seen(envelope.meta.event_id)

        # Request offset commit (picked up by poll thread)
        await self._commit_queue.put(_CommitRequest(msg=item.raw_msg))

    async def _call_handler_with_retries(
        self,
        handler: AsyncEventHandler,
        envelope: EventEnvelope,
        raw_msg: Message,
    ) -> bool:
        """
        Call a handler with exponential backoff retries.

        Returns True if the handler succeeded, False if it exhausted
        retries (and was sent to DLQ).
        """
        last_attempt = self._max_retries + 1
        for attempt in range(1, last_attempt + 1):
            try:
                await handler(envelope)
                return True
            except Exception:
                if attempt == last_attempt:
                    logger.exception(
                        "Handler %s failed after %d retries for event %s "
                        "— sending to DLQ",
                        handler.__qualname__,
                        self._max_retries,
                        envelope.meta.event_id,
                    )
                    # DLQ produce uses the producer (thread-safe in
                    # confluent_kafka — librdkafka handles its own locking)
                    self._send_to_dlq(
                        raw_msg,
                        reason=f"handler_failure:{handler.__qualname__}",
                    )
                    return False
                else:
                    backoff = min(2**attempt, 10)
                    logger.warning(
                        "Handler %s failed (attempt %d/%d) for event %s "
                        "— retrying in %.1fs",
                        handler.__qualname__,
                        attempt,
                        last_attempt,
                        envelope.meta.event_id,
                        backoff,
                    )
                    await asyncio.sleep(backoff)  # Non-blocking backoff

        return False  # Unreachable, but explicit

    # ── Idempotency tracking ──────────────────────────────────────────
    # _mark_seen is called only from the dispatch task (single async task).
    # _seen_ids is read from the poll thread via `in` check — set.__contains__
    # is atomic in CPython (GIL). If targeting free-threaded Python, add a lock.

    def _mark_seen(self, event_id: str) -> None:
        if event_id in self._seen_ids:
            return
        self._seen_ids.add(event_id)
        self._seen_ids_order.append(event_id)
        while len(self._seen_ids_order) > self._idempotency_window:
            oldest = self._seen_ids_order.pop(0)
            self._seen_ids.discard(oldest)

    # ── Helpers ───────────────────────────────────────────────────────

    def _resolve_subscribed_topics(self) -> list[str]:
        topics: set[str] = set()
        for event_type in self._handlers:
            topic = TOPIC_ROUTING.get(event_type)
            if topic:
                topics.add(topic)
            else:
                logger.warning("No topic mapping for event type %s", event_type)
        return sorted(topics)
