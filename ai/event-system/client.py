"""
EventsClient: Async Kafka producer/consumer for the event system.

Architecture:
    ┌──────────────────────┐       ┌──────────────────────────┐
    │  Thread (_poll_loop)  │──────>│  Task (_dispatch_loop)   │
    │                      │ Queue │                          │
    │  poll → deserialize  │       │  await handler(envelope) │
    │  dedup → enqueue     │       │  commit offset           │
    └──────────────────────┘       └──────────────────────────┘

    The poll thread deserializes and enqueues. The async task dispatches
    to handlers and commits offsets. Handlers are fully async.

Usage:
    client = EventsClient(
        config=KafkaConfig(),
        service_name="ui-backend",
        consumer_group="ui-backend-group",
    )
    client.on(EventType.USER_UPDATED, handle_user_update)

    await client.start()
    await client.produce(some_event)
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
rebuild_envelope()

AsyncEventHandler = Callable[[EventEnvelope], Awaitable[None]]


@dataclass(slots=True)
class _QueueItem:
    """Deserialized event + raw message (for offset commit)."""

    envelope: EventEnvelope
    raw_msg: Message


class EventsClient:
    """
    Async Kafka producer/consumer.

    Internally manages a poll thread and a dispatch task. From the
    caller's perspective, this is a pure async interface.
    """

    def __init__(
        self,
        config: KafkaConfig,
        service_name: str,
        consumer_group: str | None = None,
        *,
        max_retries: int = 3,
        queue_max_size: int = 1_000,
        idempotency_window: int = 10_000,
    ) -> None:
        self._config = config
        self._service_name = service_name
        self._consumer_group = consumer_group
        self._max_retries = max_retries

        self._handlers: dict[EventType, list[AsyncEventHandler]] = defaultdict(list)
        self._producer = Producer(config.to_producer_config())
        self._consumer: Consumer | None = None

        self._running = False
        self._loop: asyncio.AbstractEventLoop | None = None
        self._dispatch_task: asyncio.Task[None] | None = None
        self._queue: asyncio.Queue[_QueueItem | None] = asyncio.Queue(
            maxsize=queue_max_size
        )
        self._current_topics: list[str] = []
        self._resubscribe_needed = False

        # Idempotency — bounded FIFO set
        self._seen_ids: set[str] = set()
        self._seen_order: list[str] = []
        self._seen_max = idempotency_window

    # ── Registration ──────────────────────────────────────────────────

    def on(self, event_type: EventType, handler: AsyncEventHandler) -> None:
        """
        Register an async handler for an event type.

        Safe to call before or after start(). If the consumer is already
        running and this event type maps to a topic we're not yet
        subscribed to, the poll thread picks up the change on its next
        iteration.
        """
        self._handlers[event_type].append(handler)
        self._resubscribe_needed = True

    def on_many(self, event_types: list[EventType], handler: AsyncEventHandler) -> None:
        """
        Register the same handler for multiple event types.

        Batches — only one resubscription even if the new types span
        multiple new topics.
        """
        for et in event_types:
            self._handlers[et].append(handler)
        self._resubscribe_needed = True

    # ── Produce ───────────────────────────────────────────────────────

    async def produce(self, event: EventEnvelope) -> None:
        """Produce an event. Non-blocking (buffered by librdkafka)."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._produce_sync, event)

    def _produce_sync(self, event: EventEnvelope) -> None:
        topic = event.topic()
        key = event.kafka_key()

        headers = {
            "event_type": event.meta.event_type.encode(),
            "source_service": event.meta.source_service.encode(),
        }

        def _on_delivery(err: Exception | None, msg: Message) -> None:
            if err:
                logger.error("Delivery failed for %s: %s", event.meta.event_id, err)

        self._producer.produce(
            topic=topic,
            key=key.encode(),
            value=event.model_dump_json().encode(),
            headers=headers,
            callback=_on_delivery,
        )
        self._producer.poll(0)

    async def flush(self, timeout: float = 10.0) -> int:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._producer.flush, timeout)

    # ── Lifecycle ─────────────────────────────────────────────────────

    async def start(self) -> None:
        """
        Start consuming. No-op if no handlers registered (producer-only).

        Handlers registered after start() are picked up dynamically.
        If a late handler needs a new topic, resubscription happens
        automatically in on().
        """
        if self._running:
            logger.warning("start() called but already running — ignoring.")
            return
        if not self._handlers:
            logger.info("No handlers — producer-only mode.")
            return
        if not self._consumer_group:
            raise ValueError("consumer_group required when handlers are registered.")

        self._loop = asyncio.get_running_loop()
        self._current_topics = self._subscribed_topics()

        logger.info("Subscribing to %s (group=%s)", self._current_topics, self._consumer_group)

        self._consumer = Consumer(self._config.to_consumer_config(self._consumer_group))
        self._consumer.subscribe(self._current_topics)
        self._running = True

        self._loop.run_in_executor(None, self._poll_loop)
        self._dispatch_task = asyncio.create_task(
            self._dispatch_loop(),
            name=f"events-dispatch-{self._service_name}",
        )

    async def stop(self, timeout: float = 10.0) -> None:
        """Drain queue, close consumer, flush producer."""
        self._running = False
        await self._queue.put(None)  # Sentinel

        if self._dispatch_task and not self._dispatch_task.done():
            try:
                await asyncio.wait_for(self._dispatch_task, timeout=timeout)
            except asyncio.TimeoutError:
                self._dispatch_task.cancel()
                try:
                    await self._dispatch_task
                except asyncio.CancelledError:
                    pass

        if self._consumer:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._consumer.close)
            self._consumer = None

        await self.flush(timeout)
        logger.info("EventsClient stopped: %s", self._service_name)

    # ── Poll thread ───────────────────────────────────────────────────

    def _poll_loop(self) -> None:
        """
        Background thread. Only: poll, deserialize, dedup, enqueue.
        No handler calls, no retries, no commits.
        """
        assert self._consumer is not None
        assert self._loop is not None

        while self._running:
            try:
                # Check if new handlers need new topic subscriptions
                if self._resubscribe_needed:
                    self._resubscribe_needed = False
                    needed = self._subscribed_topics()
                    if needed != self._current_topics:
                        logger.info(
                            "Resubscribing: %s -> %s",
                            self._current_topics,
                            needed,
                        )
                        self._consumer.subscribe(needed)
                        self._current_topics = needed

                msg = self._consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logger.error("Consumer error: %s", msg.error())
                    continue

                raw = msg.value()
                if raw is None:
                    self._consumer.commit(msg, asynchronous=True)
                    continue

                # Deserialize
                try:
                    envelope = EventEnvelope.model_validate_json(raw)
                except Exception:
                    logger.exception(
                        "Bad message at %s/%s/%s — skipping",
                        msg.topic(), msg.partition(), msg.offset(),
                    )
                    self._consumer.commit(msg, asynchronous=True)
                    continue

                # Dedup + relevance check
                if (
                    envelope.meta.event_id in self._seen_ids
                    or envelope.meta.event_type not in self._handlers
                ):
                    self._consumer.commit(msg, asynchronous=True)
                    continue

                # Enqueue — blocks if full (backpressure)
                item = _QueueItem(envelope=envelope, raw_msg=msg)
                asyncio.run_coroutine_threadsafe(
                    self._queue.put(item), self._loop
                ).result(timeout=30.0)

            except KafkaException as exc:
                logger.error("Kafka error: %s", exc)
                time.sleep(1)
            except Exception:
                logger.exception("Poll loop error")
                time.sleep(1)

    # ── Dispatch task ─────────────────────────────────────────────────

    async def _dispatch_loop(self) -> None:
        """Async task: drain queue, call handlers, commit offsets."""
        assert self._consumer is not None

        while True:
            item = await self._queue.get()
            if item is None:
                break
            await self._handle(item)

    async def _handle(self, item: _QueueItem) -> None:
        """Dispatch one event to handlers with retries, then commit."""
        assert self._consumer is not None

        envelope = item.envelope
        handlers = self._handlers.get(envelope.meta.event_type, [])

        for handler in handlers:
            for attempt in range(1, self._max_retries + 2):
                try:
                    await handler(envelope)
                    break
                except Exception:
                    if attempt > self._max_retries:
                        logger.exception(
                            "Handler %s gave up after %d retries on %s — moving on",
                            handler.__qualname__,
                            self._max_retries,
                            envelope.meta.event_id,
                        )
                        break
                    backoff = min(2**attempt, 10)
                    logger.warning(
                        "Handler %s attempt %d/%d failed on %s — retry in %ds",
                        handler.__qualname__,
                        attempt,
                        self._max_retries + 1,
                        envelope.meta.event_id,
                        backoff,
                    )
                    await asyncio.sleep(backoff)

        self._mark_seen(envelope.meta.event_id)
        self._consumer.commit(item.raw_msg, asynchronous=True)

    # ── Idempotency ───────────────────────────────────────────────────

    def _mark_seen(self, event_id: str) -> None:
        self._seen_ids.add(event_id)
        self._seen_order.append(event_id)
        while len(self._seen_order) > self._seen_max:
            self._seen_ids.discard(self._seen_order.pop(0))

    # ── Helpers ───────────────────────────────────────────────────────

    def _subscribed_topics(self) -> list[str]:
        topics: set[str] = set()
        for et in self._handlers:
            t = TOPIC_ROUTING.get(et)
            if t:
                topics.add(t)
        return sorted(topics)