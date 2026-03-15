"""
EventsClient: Unified producer/consumer for the event system.

Design decisions:
    - Programmatic handler registration (not decorators) — consistent with
      Stan's preference and better for dynamic handler setup.
    - Handlers are registered per EventType, not per topic. The client
      manages topic subscription internally based on registered handlers.
    - Async handlers run in a thread pool to avoid blocking the Kafka
      poll loop. For CPU-bound handlers, callers can wrap in their own
      executor.
    - Idempotency tracking via an in-memory set (swap for Redis in prod
      if you need cross-restart dedup).
    - DLQ support: events that fail after max_retries are produced to
      a dead letter topic for later inspection.
    - Graceful shutdown with proper consumer group leave.

Usage:
    client = EventsClient(
        config=KafkaConfig(),
        service_name="ui-backend",
        consumer_group="ui-backend-group",
    )

    # Register handlers
    client.on(EventType.USER_UPDATED, handle_user_update)
    client.on(EventType.PERMISSIONS_UPDATED, handle_permissions_update)
    client.on(EventType.SIMULATION_PROBLEM, handle_sim_problem)

    # Start consuming (runs poll loop in background thread)
    client.start()

    # Produce events
    event = UserUpdatedEvent(
        source_service="user-service",
        user_id="usr-123",
        changed_fields=["email"],
    )
    client.produce(event)

    # Shutdown
    client.stop()
"""

from __future__ import annotations

import json
import logging
import threading
import time
from collections import defaultdict
from collections.abc import Callable
from typing import TYPE_CHECKING

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

from events_lib.config import KafkaConfig
from events_lib.models.base import (
    TOPIC_ROUTING,
    EventEnvelope,
    EventMeta,
    EventType,
    rebuild_envelope,
)

if TYPE_CHECKING:
    from confluent_kafka import Message

logger = logging.getLogger("events_lib")

# Ensure the discriminated union is built before any deserialization
rebuild_envelope()

# Type alias for event handler functions
EventHandler = Callable[[EventEnvelope], None]


class EventsClient:
    """
    Unified Kafka producer/consumer for the event notification system.

    Thread-safe. The consumer poll loop runs in a daemon thread.
    Production and consumption can happen concurrently.
    """

    def __init__(
        self,
        config: KafkaConfig,
        service_name: str,
        consumer_group: str | None = None,
        *,
        max_retries: int | None = None,
        idempotency_window: int = 10_000,
    ) -> None:
        """
        Args:
            config: Kafka connection configuration.
            service_name: Name of this service (used in event metadata).
            consumer_group: Kafka consumer group ID. None = producer-only mode.
            max_retries: Override config's max_retries_before_dlq.
            idempotency_window: Max number of event IDs to track for dedup.
        """
        self._config = config
        self._service_name = service_name
        self._consumer_group = consumer_group
        self._max_retries = max_retries or config.max_retries_before_dlq

        # Handler registry: EventType -> list of handler callables
        self._handlers: dict[EventType, list[EventHandler]] = defaultdict(list)

        # Producer (always available)
        self._producer = Producer(config.to_producer_config())

        # Consumer (lazy — created on start() if handlers are registered)
        self._consumer: Consumer | None = None
        self._running = False
        self._poll_thread: threading.Thread | None = None

        # Idempotency tracking — bounded set with FIFO eviction
        self._seen_ids: set[str] = set()
        self._seen_ids_order: list[str] = []
        self._idempotency_window = idempotency_window
        self._seen_lock = threading.Lock()

    # ── Handler registration ──────────────────────────────────────────

    def on(
        self,
        event_type: EventType,
        handler: EventHandler,
    ) -> None:
        """
        Register a handler for an event type.

        Multiple handlers per event type are supported — they execute
        sequentially. Register before calling start().
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
        handler: EventHandler,
    ) -> None:
        """Register the same handler for multiple event types."""
        for et in event_types:
            self.on(et, handler)

    # ── Production ────────────────────────────────────────────────────

    def produce(
        self,
        event: EventEnvelope,
        *,
        on_delivery: Callable[[Exception | None, Message], None] | None = None,
    ) -> None:
        """
        Produce an event to Kafka.

        The topic and partition key are derived from the event itself.
        This method is non-blocking — the event is buffered and sent
        asynchronously. Call flush() to wait for pending deliveries.
        """
        topic = event.topic()
        key = event.kafka_key()
        value = event.model_dump_json()

        # Attach event_type as a Kafka header for consumer-side filtering
        # without full deserialization (useful for monitoring tools).
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
            if on_delivery:
                on_delivery(err, msg)

        self._producer.produce(
            topic=topic,
            key=key.encode(),
            value=value.encode(),
            headers=headers,
            callback=_delivery_callback,
        )
        # Trigger delivery callbacks for previously buffered messages
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> int:
        """Wait for all buffered events to be delivered. Returns remaining."""
        return self._producer.flush(timeout)

    # ── Consumption ───────────────────────────────────────────────────

    def start(self) -> None:
        """
        Start the consumer poll loop in a background thread.

        Only subscribes to topics that have registered handlers.
        Does nothing if no handlers are registered (producer-only mode).
        """
        if not self._handlers:
            logger.info("No handlers registered — running in producer-only mode.")
            return

        if not self._consumer_group:
            raise ValueError(
                "consumer_group is required when handlers are registered."
            )

        # Determine which topics to subscribe to
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

        self._poll_thread = threading.Thread(
            target=self._poll_loop,
            name=f"events-consumer-{self._service_name}",
            daemon=True,
        )
        self._poll_thread.start()

    def stop(self, timeout: float = 10.0) -> None:
        """Gracefully shut down consumer and flush producer."""
        self._running = False

        if self._poll_thread and self._poll_thread.is_alive():
            self._poll_thread.join(timeout=timeout)

        if self._consumer:
            self._consumer.close()
            self._consumer = None

        self.flush(timeout)
        logger.info("EventsClient stopped for %s", self._service_name)

    # ── Internal ──────────────────────────────────────────────────────

    def _resolve_subscribed_topics(self) -> list[str]:
        """
        Determine which Kafka topics to subscribe to based on
        registered handlers.
        """
        topics: set[str] = set()
        for event_type in self._handlers:
            topic = TOPIC_ROUTING.get(event_type)
            if topic:
                topics.add(topic)
            else:
                logger.warning(
                    "No topic mapping for event type %s", event_type
                )
        return sorted(topics)

    def _poll_loop(self) -> None:
        """Main consumer loop — runs in a background thread."""
        assert self._consumer is not None

        while self._running:
            try:
                msg = self._consumer.poll(timeout=1.0)
                if msg is None:
                    continue

                if msg.error():
                    self._handle_consumer_error(msg)
                    continue

                self._process_message(msg)

            except KafkaException as exc:
                logger.error("Kafka consumer error: %s", exc)
                time.sleep(1)  # Back off on transient errors
            except Exception:
                logger.exception("Unexpected error in consumer loop")
                time.sleep(1)

    def _handle_consumer_error(self, msg: Message) -> None:
        """Handle Kafka-level consumer errors."""
        error = msg.error()
        if error.code() == KafkaError._PARTITION_EOF:
            # Normal — reached end of partition, will get more data later
            return
        logger.error("Consumer error: %s", error)

    def _process_message(self, msg: Message) -> None:
        """Deserialize and dispatch a single Kafka message."""
        assert self._consumer is not None

        raw_value = msg.value()
        if raw_value is None:
            self._consumer.commit(msg)
            return

        try:
            envelope = EventEnvelope.model_validate_json(raw_value)
        except Exception:
            logger.exception(
                "Failed to deserialize event from %s [%s/%s] — sending to DLQ",
                msg.topic(),
                msg.partition(),
                msg.offset(),
            )
            self._send_to_dlq(msg, reason="deserialization_error")
            self._consumer.commit(msg)
            return

        # Idempotency check
        if self._is_duplicate(envelope.meta.event_id):
            logger.debug(
                "Duplicate event %s — skipping", envelope.meta.event_id
            )
            self._consumer.commit(msg)
            return

        # Dispatch to registered handlers
        event_type = envelope.meta.event_type
        handlers = self._handlers.get(event_type, [])

        if not handlers:
            # We received an event on a shared topic that we don't handle.
            # This is normal — topics carry multiple event types.
            self._consumer.commit(msg)
            return

        retry_count = 0
        for handler in handlers:
            while retry_count <= self._max_retries:
                try:
                    handler(envelope)
                    break  # Success
                except Exception:
                    retry_count += 1
                    if retry_count > self._max_retries:
                        logger.exception(
                            "Handler %s failed after %d retries for event %s "
                            "— sending to DLQ",
                            handler.__qualname__,
                            self._max_retries,
                            envelope.meta.event_id,
                        )
                        self._send_to_dlq(
                            msg,
                            reason=f"handler_failure:{handler.__qualname__}",
                        )
                    else:
                        logger.warning(
                            "Handler %s failed (attempt %d/%d) for event %s",
                            handler.__qualname__,
                            retry_count,
                            self._max_retries,
                            envelope.meta.event_id,
                        )
                        time.sleep(min(2**retry_count, 10))  # Exponential backoff

        self._mark_seen(envelope.meta.event_id)
        self._consumer.commit(msg)

    def _is_duplicate(self, event_id: str) -> bool:
        with self._seen_lock:
            return event_id in self._seen_ids

    def _mark_seen(self, event_id: str) -> None:
        with self._seen_lock:
            if event_id in self._seen_ids:
                return
            self._seen_ids.add(event_id)
            self._seen_ids_order.append(event_id)
            # Evict oldest entries beyond the window
            while len(self._seen_ids_order) > self._idempotency_window:
                oldest = self._seen_ids_order.pop(0)
                self._seen_ids.discard(oldest)

    def _send_to_dlq(self, msg: Message, reason: str) -> None:
        """
        Produce a failed message to the dead letter queue topic.

        DLQ topic = original_topic + dlq_suffix (e.g. "user-events.dlq").
        The original message is forwarded as-is, with extra headers for
        debugging.
        """
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
