"""Kafka configuration using pydantic-settings."""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings


class KafkaConfig(BaseSettings):
    """
    Kafka connection and behavior configuration.

    Loads from environment variables with KAFKA_ prefix.
    Example:
        KAFKA_BOOTSTRAP_SERVERS=broker1:9092,broker2:9092
        KAFKA_SECURITY_PROTOCOL=SASL_SSL
        KAFKA_SASL_MECHANISM=PLAIN
        KAFKA_SASL_USERNAME=my-api-key
        KAFKA_SASL_PASSWORD=my-api-secret
    """

    model_config = {"env_prefix": "KAFKA_"}

    # Connection
    bootstrap_servers: str = "localhost:9092"
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: str | None = None
    sasl_username: str | None = None
    sasl_password: str | None = None

    # Producer tuning
    producer_acks: str = "all"
    producer_retries: int = 5
    producer_linger_ms: int = 5
    producer_batch_size: int = 16384
    enable_idempotence: bool = True

    # Consumer tuning
    consumer_auto_offset_reset: str = "latest"
    consumer_enable_auto_commit: bool = False
    consumer_max_poll_interval_ms: int = 300_000
    consumer_session_timeout_ms: int = 45_000

    # DLQ
    dlq_topic_suffix: str = ".dlq"
    max_retries_before_dlq: int = 3

    def to_producer_config(self) -> dict[str, object]:
        """Build confluent_kafka Producer config dict."""
        config: dict[str, object] = {
            "bootstrap.servers": self.bootstrap_servers,
            "security.protocol": self.security_protocol,
            "acks": self.producer_acks,
            "retries": self.producer_retries,
            "linger.ms": self.producer_linger_ms,
            "batch.size": self.producer_batch_size,
            "enable.idempotence": self.enable_idempotence,
        }
        if self.sasl_mechanism:
            config["sasl.mechanism"] = self.sasl_mechanism
        if self.sasl_username:
            config["sasl.username"] = self.sasl_username
        if self.sasl_password:
            config["sasl.password"] = self.sasl_password
        return config

    def to_consumer_config(self, group_id: str) -> dict[str, object]:
        """Build confluent_kafka Consumer config dict."""
        config: dict[str, object] = {
            "bootstrap.servers": self.bootstrap_servers,
            "security.protocol": self.security_protocol,
            "group.id": group_id,
            "auto.offset.reset": self.consumer_auto_offset_reset,
            "enable.auto.commit": self.consumer_enable_auto_commit,
            "max.poll.interval.ms": self.consumer_max_poll_interval_ms,
            "session.timeout.ms": self.consumer_session_timeout_ms,
        }
        if self.sasl_mechanism:
            config["sasl.mechanism"] = self.sasl_mechanism
        if self.sasl_username:
            config["sasl.username"] = self.sasl_username
        if self.sasl_password:
            config["sasl.password"] = self.sasl_password
        return config
